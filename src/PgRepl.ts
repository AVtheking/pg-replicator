import { Cause, Data, Effect, Queue, Stream } from "effect"
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

export interface PgRepl {
    createReplicationSlot(option: CreateReplicationSlot): Effect.Effect<CreateReplicationSlotResult, PgReplError, never>
    startReplication(option: StartReplicationOption): Stream.Stream<Buffer, PgReplError>
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

            })
    }
}

