import { Cause, Data, Effect, Queue, Stream, Array, Match } from "effect"
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
    flag: string
    name: string
    dataTypeOID: number
    dataTypeModifier: number
}

type RelationData = {
    relationId: number
    namespace: string
    name: string
    replicaIdentity: string
    numberOfColumns: number
    relationColumns: RelationColumn[]
}

export type PgOutput = Data.TaggedEnum<{
    keepalive: {
        serverWalEnd: bigint
        serverTime: bigint
        replyRequested: boolean
    }
    Begin: {
        finalLSN: bigint
        commitTimestamp: bigint
        xid: number
    }
    Relation: {
        relationId: number
        namespace: string
        name: string
        replicaIdentity: string
        numberOfColumns: number
        relationColumns: RelationColumn[]
    }
    Insert: {
        relationId: number
        tupleData: TupleData
    }
    Unknown: {
        type: string
        walData: Buffer
    }
}>

export const PgOutput = Data.taggedEnum<PgOutput>()



const decodeCopyData = (chunk: Buffer): Effect.Effect<CopyData, PgReplError, never> => {
    const code = String.fromCharCode(chunk[0])
    switch (code) {
        case CopyDataCode.Keepalive:
            return Effect.succeed(CopyData.Keepalive({
                serverWalEnd: chunk.readBigUInt64BE(1),
                serverTime: chunk.readBigUInt64BE(9),
                replyRequested: chunk[17] !== 0
            }))
        case CopyDataCode.XLogData:
            return Effect.succeed(CopyData.XLogData({
                serverWalStart: chunk.readBigUInt64BE(1),
                serverWalEnd: chunk.readBigUInt64BE(9),
                serverTime: chunk.readBigUInt64BE(17),
                walData: chunk.subarray(25)
            }))
        default:
            return Effect.fail(new PgReplError({ message: `unknown copy data code: ${code}`, cause: chunk }))
    }
}

const decodeTupleData = (tupleData: Buffer): TupleData => {
    let offset = 0
    const numberOfColumns = tupleData.readUInt16BE(offset)
    offset += 2

    const columns = numberOfColumns === 0 ? [] : Array.makeBy<Column>(numberOfColumns, () => {
        const dataType = String.fromCharCode(tupleData[offset++])
        let length = 0;
        switch (dataType) {
            case "n":
                return Column.Null()
            case "u":
                return Column.Toast()
            case "t":
                length = tupleData.readUInt32BE(offset)
                offset += 4
                const value = tupleData.subarray(offset, offset + length)
                offset += length
                return Column.Text({ value: value.toString("utf-8") })
            case "b":
                length = tupleData.readUInt32BE(offset)
                offset += 4
                const binaryValue = tupleData.subarray(offset, offset + length)
                offset += length
                return Column.Binary({ value: binaryValue })
            default:
                throw new PgReplError({ message: `unknown data type: ${dataType}` })
        }
    })

    return {
        numberOfColumns,
        columns
    }
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

    const replicaIdentity = String.fromCharCode(relationData[offset++])
    const numberOfColumns = relationData.readUInt16BE(offset)
    offset += 2

    const columns = numberOfColumns === 0 ? [] : Array.makeBy<RelationColumn>(numberOfColumns, () => {
        const flag = String.fromCharCode(relationData[offset++])
        const [name, columnNameLen] = readCString(relationData, offset)
        offset = columnNameLen

        const dataTypeOID = relationData.readUInt32BE(offset)
        offset += 4

        const dataTypeModifier = relationData.readUInt32BE(offset)
        offset += 4

        return { flag, name, dataTypeOID, dataTypeModifier }
    })

    return { relationId, namespace, name, replicaIdentity, numberOfColumns, relationColumns: columns }

}


const decodeWalData = (walData: Buffer): Effect.Effect<PgOutput, PgReplError> => {
    const firstByte = String.fromCharCode(walData[0])
    switch (firstByte) {
        case "R":
            return Effect.succeed(PgOutput.Relation(decodeRelatioData(walData.subarray(1))))

        case "B":
            return Effect.succeed(PgOutput.Begin({
                finalLSN: walData.readBigUInt64BE(1),
                commitTimestamp: walData.readBigUInt64BE(9),
                xid: walData.readUInt32BE(17),
            }))

        case "I":
            if (String.fromCharCode(walData[5]) !== "N") {
                return Effect.fail(new PgReplError({ message: "INSERT: expected 'N' tuple" }))
            }
            return Effect.succeed(PgOutput.Insert({
                relationId: walData.readUInt32BE(1),
                tupleData: decodeTupleData(walData.subarray(6)),
            }))

        default:
            return Effect.logInfo(`Not implemented this byte: ${firstByte}`).pipe(
                Effect.as(PgOutput.Unknown({ type: firstByte, walData }))
            )
    }
}

const decodePgOutput = Match.type<CopyData>().pipe(
    Match.tag("Keepalive", (k) =>
        Effect.succeed(PgOutput.keepalive({
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
                        console.error(error)
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
            }).pipe(Stream.mapEffect(decodeCopyData),
                Stream.mapEffect(decodePgOutput))
    }
}

