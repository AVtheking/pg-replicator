import { Cause, Effect, Queue, Stream, Match, Result, Ref } from "effect"
import pg, { Connection } from "pg"
import { PgReplError, SlotAlreadyExists } from "./errors"
import { dateToPgTime } from "./utils"
import { Column, decodeCopyData, decodePgOutput, formatLSN, parseLSN, PgOutput, RelationData, TupleData, } from "./message"

export enum ReplicationMode {
    Logical = "LOGICAL",
    // Physical = "PHYSICAL",
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

export type TextDecoder = (text: string) => unknown
export const defaultDecoders: ReadonlyMap<number, TextDecoder> = new Map<number, TextDecoder>([
    [16, (t) => t === "t"],           // bool
    [21, Number],                     // int2
    [23, Number],                     // int4
    [20, BigInt],                     // int8
    [700, Number],                     // float4
    [701, Number],                     // float8
    [1114, (t) => new Date(t + "Z")],   // timestamp (no tz, treat as UTC)
    [1184, (t) => new Date(t)],         // timestamptz
    [114, JSON.parse],                 // json
    [3802, JSON.parse],                 // jsonb
])

const convertToRows = (relation: RelationData, tupleData: TupleData) => {
    const rows: Record<string, unknown> = {}
    relation.relationColumns.forEach((column, i) => {
        const tuple = tupleData.columns[i]
        const decode = defaultDecoders.get(column.dataTypeOID) ?? (t => t)

        Column.$match(tuple, {
            Null: () => rows[column.name] = null,
            Toast: () => rows[column.name] = "(unchanged)",
            Text: (t) => rows[column.name] = decode(t.value),
            Binary: (b) => rows[column.name] = b.value,
        })
    })

    return rows
}

interface ReplicationConnection extends Connection {
    sendCopyFromChunk(chunk: Buffer): void
    endCopyFrom(): void
}

export interface PgReplicator {
    createReplicationSlot(option: CreateReplicationSlot): Effect.Effect<CreateReplicationSlotResult, PgReplError | SlotAlreadyExists, never>
    startReplication(option: StartReplicationOption): Stream.Stream<PgOutput, PgReplError>
    dropReplicationSlot(slotName: string): Effect.Effect<void, PgReplError, never>
    ack(lsn: bigint): Effect.Effect<void, PgReplError, never>
}

export const fromPg = (client: pg.Client): Effect.Effect<PgReplicator> => Effect.gen(function* () {

    const lastAckedLSN = yield* Ref.make<bigint | null>(null)

    const sendStatus = (replyRequested = false) =>
        Effect.gen(function* () {
            const lsn = yield* Ref.get(lastAckedLSN)
            if (lsn === null) return
            (client.connection as ReplicationConnection).sendCopyFromChunk(encodeStandByStatusUpdate(lsn, replyRequested))
        })

    return {
        ack: (lsn: bigint) => Ref.set(lastAckedLSN, lsn).pipe(Effect.andThen(() => sendStatus())),
        createReplicationSlot: Effect.fn(function* (option: CreateReplicationSlot) {
            const temporaryStr = option.options?.temporary ? "TEMPORARY" : ""
            const mode = option.options?.mode ?? ReplicationMode.Logical

            const sql = `CREATE_REPLICATION_SLOT ${option.slotName} ${temporaryStr} ${mode} ${option.outputPlugin}`
            const result = yield* Effect.tryPromise({
                try: () => client.query(sql),
                catch: (error) =>
                    (error as any).code === "42710"
                        ? new SlotAlreadyExists({ slotName: option.slotName })
                        : new PgReplError({ message: `command failed : ${sql}`, cause: error })
            })


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
        dropReplicationSlot: (slotName: string) =>
            Effect.gen(function* () {
                yield* Effect.tryPromise({
                    try: () => client.query(`DROP_REPLICATION_SLOT ${slotName}`),
                    catch: (error) => new PgReplError({ message: `command failed : DROP_REPLICATION_SLOT ${slotName}`, cause: error })
                })
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

                    yield* Effect.addFinalizer(() =>
                        Effect.gen(function* () {
                            yield* sendStatus();
                            (client.connection as ReplicationConnection).endCopyFrom()
                        }).pipe(Effect.ignore))

                    yield* Ref.set(lastAckedLSN, option.startLSN)

                    const mode = option.mode ? option.mode : "LOGICAL"

                    const sql = `START_REPLICATION SLOT ${option.slot} ${mode} ${formatLSN(option.startLSN)} (proto_version '${option.protoVersion}', publication_names '${option.publication}')`
                    //not awaited
                    client.query(sql).catch((error) =>
                        Queue.failCauseUnsafe(queue, Cause.fail(new PgReplError({ message: `command failed : ${sql}`, cause: error }))))
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
                        return Match.value(msg).pipe(
                            Match.withReturnType<readonly [Map<number, RelationData>, ReadonlyArray<PgOutput>]>(),
                            Match.tag("Relation", (relation) => {
                                relations.set(relation.relationId, relation)
                                return [relations, [relation]]
                            }),
                            Match.tag("Insert", (insert) => {
                                const relation = relations.get(insert.relationId)
                                if (!relation) return [relations, []]

                                const rows = convertToRows(relation, insert.tupleData)
                                return [relations, [PgOutput.Insert({ ...insert, rows })]]
                            }),
                            Match.tag("Update", (update) => {
                                const relation = relations.get(update.relationId)

                                if (!relation) return [relations, []]

                                let oldRows: Record<string, unknown> | undefined
                                if (update.oldTupleData) {
                                    oldRows = convertToRows(relation, update.oldTupleData)
                                }
                                const newRows = convertToRows(relation, update.newTupleData)
                                return [relations, [PgOutput.Update({ ...update, oldRows, newRows })]]
                            }),
                            Match.tag("Delete", (del) => {
                                const relation = relations.get(del.relationId)

                                if (!relation) return [relations, []]
                                let oldRows: Record<string, unknown> | undefined

                                if (del.oldTupleData) {
                                    oldRows = convertToRows(relation, del.oldTupleData)
                                }
                                return [relations, [PgOutput.Delete({ ...del, rows: oldRows })]]
                            }),
                            Match.orElse(() => [relations, [msg]])
                        )
                    })
            )
    }
})

