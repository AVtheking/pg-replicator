import { Cause, Data, Effect, Queue, Stream, Array, Match, Result } from "effect"
import pg from "pg"

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
    consistentPoint: string
    snapshotName: string | null
    outputPlugin: string
}

export interface StartReplicationOption {
    slot: string
    startLSN: string
    publication: string
    protoVersion: number
    mode?: ReplicationMode
}

export enum CopyDataCode {
    Keepalive = "k",
    XLogData = "w",
}

export type CopyData = Data.TaggedEnum<{
    Keepalive: {
        serverWalEnd: bigint
        serverTime: bigint
        replyRequested: boolean
    }
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
    Keepalive: {
        serverWalEnd: bigint
        serverTime: bigint
        replyRequested: boolean
    }
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
    Relation: {
        relationId: number
        namespace: string
        name: string
        replicaIdentity: number
        numberOfColumns: number
        relationColumns: RelationColumn[]
    }
    Insert: {
        relationId: number
        tupleData: TupleData
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

const decodeCopyData = (chunk: Buffer): Result.Result<CopyData, PgReplError> => {
    const code = String.fromCharCode(chunk[0])
    switch (code) {
        case CopyDataCode.Keepalive:
            return Result.succeed(CopyData.Keepalive({
                serverWalEnd: chunk.readBigUInt64BE(1),
                serverTime: chunk.readBigUInt64BE(9),
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

const decodeTupleData = (tupleData: Buffer): Result.Result<TupleData, PgReplError> => {
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
        numberOfColumns,
        columns
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
                    tupleData: tupleData,
                }))

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




export interface PgRepl {
    createReplicationSlot(option: CreateReplicationSlot): Effect.Effect<CreateReplicationSlotResult, PgReplError, never>
    startReplication(option: StartReplicationOption): Stream.Stream<PgOutput, PgReplError>
}

export const fromPg = (client: pg.Client): PgRepl => {

    const runCommand = (sql: string) =>
        Effect.tryPromise({
            try: () => client.query(sql),
            catch: (error) => new PgReplError({ message: `command failed : ${sql}`, cause: error })
        })

    return {
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
                consistentPoint: row.consistent_point,
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

                    const sql = `START_REPLICATION SLOT ${option.slot} ${mode} ${option.startLSN} (proto_version '${option.protoVersion}', publication_names '${option.publication}')`

                    //not awaited
                    client.query(sql).catch((error) => Queue.failCauseUnsafe(queue, Cause.fail(new PgReplError({ message: `command failed : ${sql}`, cause: error }))))
                })
            }).pipe(
                Stream.mapEffect((chunk) =>
                    Effect.fromResult(Result.flatMap(decodeCopyData(chunk), decodePgOutput))),
                Stream.tap((msg) =>
                    msg._tag === "Unknown"
                        ? Effect.logInfo(`Not implemented this byte: ${msg.type}`)
                        : Effect.void
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

                                const rows: Record<string, unknown> = {}
                                relation.relationColumns.forEach((column, i) => {
                                    const tuple = msg.tupleData.columns[i]
                                    Column.$match(tuple, {
                                        Null: () => rows[column.name] = null,
                                        Toast: () => rows[column.name] = "(unchanged)",
                                        Text: (t) => rows[column.name] = t.value,
                                        Binary: (b) => rows[column.name] = b.value,
                                    })
                                })

                                return [relations, [PgOutput.Insert({ ...msg, rows })]]
                            }
                            default:
                                return [relations, [msg]]
                        }
                    }

                )
            )
    }
}

