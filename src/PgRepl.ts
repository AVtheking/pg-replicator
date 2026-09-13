import { Cause, Data, Effect, Queue, Stream, Array } from "effect"
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

type CopyData =
    | {
        readonly _tag: "keepalive"
        readonly serverWalEnd: bigint
        readonly serverTime: bigint
        readonly replyRequested: boolean
    }
    | {
        readonly _tag: "xLogData"
        readonly serverWalStart: bigint
        readonly serverWalEnd: bigint
        readonly serverTime: bigint
        readonly walData: Buffer
    }

type Column = {
    dataType: string
    length: number
    data: Uint8Array
}

type TupleData = {
    numberOfColumns: number
    columns: Column[]
}

type PgOutput =
    | {
        _tag: "keepalive"
        serverWalEnd: bigint
        serverTime: bigint
        replyRequested: boolean
    }
    | {
        _tag: "BEGIN"
        finalLSN: bigint
        commitTimestamp: bigint
        xid: number
    }
    | {
        _tag: "INSERT"
        relationId: number
        tupleData: TupleData
    }
    | {
        _tag: "UNKNOWN"
        type: string
        walData: Uint8Array
    }

const decodeCopyData = (chunk: Buffer): Effect.Effect<CopyData, PgReplError, never> => {
    const code = String.fromCharCode(chunk[0])
    switch (code) {
        case CopyDataCode.Keepalive:
            return Effect.succeed({
                _tag: "keepalive",
                serverWalEnd: chunk.readBigUInt64BE(1),
                serverTime: chunk.readBigUInt64BE(9),
                replyRequested: chunk[17] !== 0
            })
        case CopyDataCode.XLogData:
            return Effect.succeed({
                _tag: "xLogData",
                serverWalStart: chunk.readBigUInt64BE(1),
                serverWalEnd: chunk.readBigUInt64BE(9),
                serverTime: chunk.readBigUInt64BE(17),
                walData: chunk.subarray(25)
            })
        default:
            return Effect.fail(new PgReplError({ message: `unknown copy data code: ${code}`, cause: chunk }))
    }
}

const decodeTupleData = (tupleData: Buffer): TupleData => {
    let offset = 0
    const numberOfColumns = tupleData.readUInt16BE(offset)
    offset += 2

    const columns = numberOfColumns === 0 ? [] : Array.makeBy<Column>(numberOfColumns, (i) => {
        const dataType = String.fromCharCode(tupleData[offset++])
        const length = tupleData.readUInt32BE(offset)
        offset += 4
        const data = tupleData.subarray(offset, offset + length)
        offset += length
        return {
            dataType,
            length,
            data
        }
    })

    return {
        numberOfColumns,
        columns
    }
}


const decodePgOutput = (copyData: CopyData): Effect.Effect<PgOutput, PgReplError, never> => {
    switch (copyData._tag) {
        case "keepalive":
            return Effect.succeed({
                _tag: "keepalive",
                serverWalEnd: copyData.serverWalEnd,
                serverTime: copyData.serverTime,
                replyRequested: copyData.replyRequested
            })
        case "xLogData":
            const walData = copyData.walData
            const firstByte = String.fromCharCode(walData[0])
            switch (firstByte) {
                case "B":
                    return Effect.succeed({
                        _tag: "BEGIN",
                        finalLSN: walData.readBigUInt64BE(1),
                        commitTimestamp: walData.readBigUInt64BE(9),
                        xid: walData.readUInt32BE(17),
                    })
                case "I":
                    if (String.fromCharCode(walData[5]) !== "N") {
                        return Effect.fail(new PgReplError({ message: "INSERT: expected 'N' tuple" }))
                    }
                    return Effect.succeed({
                        _tag: "INSERT",
                        relationId: walData.readUInt32BE(1),
                        tupleData: decodeTupleData(walData.subarray(6))

                    })
                default:
                    return Effect.logInfo("Not implemented this byte: " + firstByte).pipe(
                        Effect.as({ _tag: "UNKNOWN", type: firstByte, walData: walData })
                    )
            }
    }
}



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

