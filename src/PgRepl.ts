import { Cause, Data, Effect, Queue, Stream, Array, Match, Result, Ref, Schedule } from "effect"
import pg, { Connection } from "pg"

export class PgReplError extends Data.TaggedError("PgReplError")<{
    readonly message: string
    readonly cause?: unknown
}> { }


export enum ReplicationMode {
    Logical = "LOGICAL",
    Physical = "PHYSICAL",
}

export interface CreateReplicationSlot {
    slotName: string
    outputPlugin: string
    options?: {
        temporary?: boolean
        mode?: ReplicationMode
    }
}

export interface CreateReplicationSlotResult {
    name: string
    consistentPoint: bigint
    snapshotName: string | null
    outputPlugin: string
}

export interface StartReplicationOption {
    slot: string
    startLSN: bigint
    publication: string
    protoVersion: number
    mode?: ReplicationMode
}

export enum CopyDataCode {
    Keepalive = "k",
    XLogData = "w",
}

export interface Keepalive {
    serverWalEnd: bigint
    serverTime: Date
    replyRequested: boolean
}

export type CopyData = Data.TaggedEnum<{
    Keepalive: Keepalive
    XLogData: {
        serverWalStart: bigint
        serverWalEnd: bigint
        serverTime: bigint
        walData: Buffer
    }
}>

export const CopyData = Data.taggedEnum<CopyData>()

export type Column = Data.TaggedEnum<{
    Null: {}
    Toast: {}
    Text: { value: string }
    Binary: { value: Uint8Array }
}>

export const Column = Data.taggedEnum<Column>()

type TupleData = {
    numberOfColumns: number
    columns: Column[]
}

type RelationColumn = {
    flag: number
    name: string
    dataTypeOID: number
    dataTypeModifier: number
}

type RelationData = {
    relationId: number
    namespace: string
    name: string
    replicaIdentity: number
    numberOfColumns: number
    relationColumns: RelationColumn[]
}

export type PgOutput = Data.TaggedEnum<{
    Keepalive: Keepalive
    Begin: {
        finalLSN: bigint
        commitTimestamp: bigint
        xid: number
    }
    Commit: {
        flags: number
        commitLSN: bigint
        endLSN: bigint
        commitTimestamp: Date
    }
    Relation: RelationData
    Insert: {
        relationId: number
        tupleData: TupleData
        rows?: Record<string, unknown>
    }
    Update: {
        xid?: number
        relationId: number
        oldTupleKind?: "K" | "O"
        oldTupleData?: TupleData
        newTupleData: TupleData
        oldRows?: Record<string, unknown>
        newRows?: Record<string, unknown>
    }
    Delete: {
        xid?: number
        relationId: number
        oldTupleKind?: "K" | "O"
        oldTupleData?: TupleData
        rows?: Record<string, unknown>
    }
    Unknown: {
        type: string
        walData: Buffer
    }
}>

export const PgOutput = Data.taggedEnum<PgOutput>()

// Postgres epoch (2000-01-01T00:00:00Z) minus Unix epoch, in microseconds
const PG_EPOCH_OFFSET_US = 946_684_800_000_000n

const pgTimeToDate = (time: bigint): Date => new Date(Number((time + PG_EPOCH_OFFSET_US) / 1000n))
const dateToPgTime = (date: Date): bigint => (BigInt(date.getTime()) * 1000n - PG_EPOCH_OFFSET_US)

export const parseLSN = (lsn: string): bigint => {
    const [hi, lo] = lsn.split("/")
    return (BigInt(`0x${hi}`) << 32n) | BigInt(`0x${lo}`)
}

export const formatLSN = (lsn: bigint): string =>
    `${(lsn >> 32n).toString(16).toUpperCase()}/${(lsn & 0xFFFFFFFFn).toString(16).toUpperCase()}`

const encodeStandByStatusUpdate = (lsn: bigint, replyRequested = false): Buffer => {
    const buf = Buffer.alloc(34)

    buf.writeUint8(0x72, 0)
    buf.writeBigUInt64BE(lsn, 1)
    buf.writeBigInt64BE(lsn, 9)
    buf.writeBigInt64BE(lsn, 17)
    buf.writeBigUInt64BE(dateToPgTime(new Date()), 25)
    buf.writeUint8(replyRequested ? 1 : 0, 33)

    return buf
}

const decodeCopyData = (chunk: Buffer): Result.Result<CopyData, PgReplError> => {
    const code = String.fromCharCode(chunk[0])
    switch (code) {
        case CopyDataCode.Keepalive:
            return Result.succeed(CopyData.Keepalive({
                serverWalEnd: chunk.readBigUInt64BE(1),
                serverTime: pgTimeToDate(chunk.readBigUInt64BE(9)),
                replyRequested: chunk[17] !== 0
            }))
        case CopyDataCode.XLogData:
            return Result.succeed(CopyData.XLogData({
                serverWalStart: chunk.readBigUInt64BE(1),
                serverWalEnd: chunk.readBigUInt64BE(9),
                serverTime: chunk.readBigUInt64BE(17),
                walData: chunk.subarray(25)
            }))
        default:
            return Result.fail(new PgReplError({ message: `unknown copy data code: ${code}`, cause: chunk }))
    }
}

const decodeTupleData = (tupleData: Buffer): Result.Result<{ tupleData: TupleData, finalOffset: number }, PgReplError> => {
    let offset = 0
    const numberOfColumns = tupleData.readUInt16BE(offset)
    offset += 2

    const columns: Column[] = []
    for (let i = 0; i < numberOfColumns; i++) {
        const dataType = String.fromCharCode(tupleData[offset++])
        switch (dataType) {
            case "n":
                columns.push(Column.Null())
                break
            case "u":
                columns.push(Column.Toast())
                break
            case "t":
            case "b": {
                const length = tupleData.readUInt32BE(offset)
                offset += 4
                const value = tupleData.subarray(offset, offset + length)
                offset += length
                columns.push(dataType === "t"
                    ? Column.Text({ value: value.toString("utf-8") })
                    : Column.Binary({ value }))
                break
            }
            default:
                return Result.fail(new PgReplError({ message: `unknown data type: ${dataType}` }))
        }
    }

    return Result.succeed({
        tupleData: { numberOfColumns, columns },
        finalOffset: offset
    })
}

const readCString = (buf: Buffer, offset: number): [string, number] => {
    const end = buf.indexOf(0, offset)
    const string = buf.toString("utf-8", offset, end)
    return [string, end + 1]
}

const decodeRelatioData = (relationData: Buffer): RelationData => {
    let offset = 0
    const relationId = relationData.readUInt32BE(offset)
    offset += 4
    const [namespace, namespaceLen] = readCString(relationData, offset)
    offset = namespaceLen

    const [name, nameLen] = readCString(relationData, offset)
    offset = nameLen

    const replicaIdentity = relationData.readUInt8(offset++)
    const numberOfColumns = relationData.readUInt16BE(offset)
    offset += 2

    const columns = numberOfColumns === 0 ? [] : Array.makeBy<RelationColumn>(numberOfColumns, () => {
        const flag = relationData.readUInt8(offset++)
        const [name, columnNameLen] = readCString(relationData, offset)
        offset = columnNameLen

        const dataTypeOID = relationData.readUInt32BE(offset)
        offset += 4

        const dataTypeModifier = relationData.readInt32BE(offset)
        offset += 4

        return { flag, name, dataTypeOID, dataTypeModifier }
    })

    return { relationId, namespace, name, replicaIdentity, numberOfColumns, relationColumns: columns }

}


const decodeWalData = (walData: Buffer): Result.Result<PgOutput, PgReplError> => {
    const firstByte = String.fromCharCode(walData[0])
    switch (firstByte) {
        case "R":
            return Result.succeed(PgOutput.Relation(decodeRelatioData(walData.subarray(1))))

        case "B":
            return Result.succeed(PgOutput.Begin({
                finalLSN: walData.readBigUInt64BE(1),
                commitTimestamp: walData.readBigUInt64BE(9),
                xid: walData.readUInt32BE(17),
            }))
        case "C":
            return Result.succeed(PgOutput.Commit({
                flags: walData.readUInt8(1),
                commitLSN: walData.readBigUInt64BE(2),
                endLSN: walData.readBigUInt64BE(10),
                commitTimestamp: pgTimeToDate(walData.readBigUInt64BE(18)),
            }))

        case "I":
            if (String.fromCharCode(walData[5]) !== "N") {
                return Result.fail(new PgReplError({ message: "INSERT: expected 'N' tuple" }))
            }
            return Result.map(decodeTupleData(walData.subarray(6)), (tupleData) =>
                PgOutput.Insert({
                    relationId: walData.readUInt32BE(1),
                    tupleData: tupleData.tupleData,
                }))
        case "U": {
            const relationId = walData.readUInt32BE(1)
            let offset = 5
            let marker = String.fromCharCode(walData[offset++])
            let oldTupleData: TupleData | undefined
            let oldTupleKind: "K" | "O" | undefined

            if (marker === "K" || marker === "O") {
                const result = decodeTupleData(walData.subarray(offset))

                if (Result.isFailure(result)) {
                    return Result.fail(new PgReplError({ message: `UPDATE: failed to decode old tuple`, cause: result }))
                }

                oldTupleData = result.success.tupleData
                oldTupleKind = marker
                offset += result.success.finalOffset
                marker = String.fromCharCode(walData[offset++])
            }

            if (marker !== "N") {
                return Result.fail(new PgReplError({ message: `UPDATE: expected 'N' tuple, got ${marker}` }))
            }

            return Result.map(decodeTupleData(walData.subarray(offset)), (result) => PgOutput.Update({
                relationId,
                oldTupleKind,
                oldTupleData,
                newTupleData: result.tupleData,
            }))
        }
        case "D": {
            const relationId = walData.readUInt32BE(1)
            const marker = String.fromCharCode(walData[5])

            if (marker === "K" || marker === "O") {
                const result = decodeTupleData(walData.subarray(6))

                if (Result.isFailure(result)) {
                    return Result.fail(new PgReplError({ message: `DELETE: failed to decode old tuple`, cause: result }))
                }
                return Result.succeed(PgOutput.Delete({
                    relationId,
                    oldTupleKind: marker,
                    oldTupleData: result.success.tupleData,
                }))
            }
        }
        default:
            return Result.succeed(PgOutput.Unknown({ type: firstByte, walData }))
    }
}

const decodePgOutput = Match.type<CopyData>().pipe(
    Match.withReturnType<Result.Result<PgOutput, PgReplError>>(),
    Match.tag("Keepalive", (k) =>
        Result.succeed(PgOutput.Keepalive({
            serverWalEnd: k.serverWalEnd,
            serverTime: k.serverTime,
            replyRequested: k.replyRequested,
        }))),
    Match.tag("XLogData", (x) => decodeWalData(x.walData)),
    Match.exhaustive,
)



interface ReplicationConnection extends Connection {
    sendCopyFromChunk(chunk: Buffer): void
}


export interface PgRepl {
    createReplicationSlot(option: CreateReplicationSlot): Effect.Effect<CreateReplicationSlotResult, PgReplError, never>
    startReplication(option: StartReplicationOption): Stream.Stream<PgOutput, PgReplError>
    ack(lsn: bigint): Effect.Effect<void, PgReplError, never>
}

export const fromPg = (client: pg.Client): Effect.Effect<PgRepl> => Effect.gen(function* () {

    const lastAckedLSN = yield* Ref.make<bigint | null>(null)

    const sendStatus = (replyRequested = false) =>
        Effect.gen(function* () {
            const lsn = yield* Ref.get(lastAckedLSN)
            if (lsn === null) return
            (client.connection as ReplicationConnection).sendCopyFromChunk(encodeStandByStatusUpdate(lsn, replyRequested))
        })

    const runCommand = (sql: string) =>
        Effect.tryPromise({
            try: () => client.query(sql),
            catch: (error) => new PgReplError({ message: `command failed : ${sql}`, cause: error })
        })

    return {
        ack: (lsn: bigint) => Ref.set(lastAckedLSN, lsn).pipe(Effect.andThen(() => sendStatus())),

        createReplicationSlot: Effect.fn(function* (option: CreateReplicationSlot) {
            const temporaryStr = option.options?.temporary ? "TEMPORARY" : ""
            const mode = option.options?.mode ?? ReplicationMode.Logical

            const result = yield* runCommand(`CREATE_REPLICATION_SLOT ${option.slotName} ${temporaryStr} ${mode} ${option.outputPlugin}`)

            if (result.rows.length > 1) {
                return yield* Effect.fail(
                    new PgReplError({ message: `expected 1 row, got ${result.rows.length}` }))
            }

            const row = result.rows[0]
            return {
                name: row.slot_name,
                consistentPoint: parseLSN(row.consistent_point),
                snapshotName: row.snapshot_name ?? null,
                outputPlugin: row.output_plugin
            }

        }),

        startReplication: (option: StartReplicationOption) =>
            Stream.callback<Buffer, PgReplError>((queue) => {
                return Effect.gen(function* () {

                    const onCopyData = (msg: { chunk: Buffer }) => {
                        Queue.offerUnsafe(queue, msg.chunk)
                    }

                    const onCopyDone = () => {
                        Queue.endUnsafe(queue)
                    }

                    const onError = (error: Error) => {
                        Queue.failCauseUnsafe(queue, Cause.fail(new PgReplError({ message: `connection error`, cause: error })))
                    }
                    const onEnd = () => {
                        Queue.endUnsafe(queue)
                    }

                    client.connection.on("copyData", onCopyData)
                    client.connection.on("copyDone", onCopyDone)
                    client.connection.on("error", onError)
                    client.connection.on("end", onEnd)
                    client.on("error", onError)

                    yield* Effect.addFinalizer(() =>
                        Effect.sync(() => {
                            client.connection.off("copyData", onCopyData)
                            client.connection.off("copyDone", onCopyDone)
                            client.connection.off("error", onError)
                            client.connection.off("end", onEnd)
                            client.off("error", onError)
                        })
                    )

                    const mode = option.mode ? option.mode : "LOGICAL"

                    const sql = `START_REPLICATION SLOT ${option.slot} ${mode} ${formatLSN(option.startLSN)} (proto_version '${option.protoVersion}', publication_names '${option.publication}')`

                    //not awaited
                    client.query(sql).catch((error) => Queue.failCauseUnsafe(queue, Cause.fail(new PgReplError({ message: `command failed : ${sql}`, cause: error }))))

                    yield* Ref.set(lastAckedLSN, option.startLSN)
                        // yield* sendStatus().pipe(
                        //     Effect.repeat(Schedule.spaced("10 seconds")),
                        //     Effect.forkScoped
                        // )
                })
            }).pipe(
                Stream.mapEffect((chunk) =>
                    Effect.fromResult(Result.flatMap(decodeCopyData(chunk), decodePgOutput))),
                Stream.tap((msg) =>
                    Match.value(msg).pipe(
                        Match.withReturnType<Effect.Effect<void, never, never>>(),
                        Match.tag("Keepalive", (k) => {
                            if (k.replyRequested) {
                                return sendStatus()
                            }
                            return Effect.void
                        }),
                        Match.tag("Unknown", (u) => {
                            return Effect.logInfo(`Not implemented this byte: ${u.type}`)
                        }),
                        Match.orElse(() => Effect.void)
                    )
                ),
                Stream.mapAccum(
                    (): Map<number, RelationData> => new Map(),
                    (relations, msg): readonly [Map<number, RelationData>, ReadonlyArray<PgOutput>] => {
                        switch (msg._tag) {
                            case "Relation": {
                                relations.set(msg.relationId, msg)
                                return [relations, [msg]]
                            }
                            case "Insert": {
                                const relation = relations.get(msg.relationId)

                                if (!relation) {
                                    return [relations, []]
                                }

                                const rows = convertToRows(relation, msg.tupleData)
                                return [relations, [PgOutput.Insert({ ...msg, rows })]]
                            }
                            case "Update": {
                                const relation = relations.get(msg.relationId)
                                if (!relation) {
                                    return [relations, []]
                                }

                                let oldRows: Record<string, unknown> | undefined
                                if (msg.oldTupleData) {
                                    oldRows = convertToRows(relation, msg.oldTupleData)
                                }

                                const newRows = convertToRows(relation, msg.newTupleData)
                                return [relations, [PgOutput.Update({ ...msg, oldRows, newRows })]]
                            }
                            case "Delete": {
                                const relation = relations.get(msg.relationId)
                                if (!relation) {
                                    return [relations, []]
                                }

                                let oldRows: Record<string, unknown> | undefined
                                if (msg.oldTupleData) {
                                    oldRows = convertToRows(relation, msg.oldTupleData)
                                }

                                return [relations, [PgOutput.Delete({ ...msg, rows: oldRows })]]
                            }
                            default:
                                return [relations, [msg]]
                        }
                    }

                )
            )
    }
})


const convertToRows = (relation: RelationData, tupleData: TupleData) => {
    const rows: Record<string, unknown> = {}
    relation.relationColumns.forEach((column, i) => {
        const tuple = tupleData.columns[i]
        Column.$match(tuple, {
            Null: () => rows[column.name] = null,
            Toast: () => rows[column.name] = "(unchanged)",
            Text: (t) => rows[column.name] = t.value,
            Binary: (b) => rows[column.name] = b.value,
        })
    })

    return rows
}

