import { Data, Effect, Stream } from "effect"
import { SqlClient } from "effect/unstable/sql"
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
}

export interface PgRepl {
    createReplicationSlot(option: CreateReplicationSlot): Effect.Effect<CreateReplicationSlotResult, PgReplError, never>
    startReplication(option: StartReplicationOption): Stream.Stream<any, never, never>
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

        startReplication: (option: StartReplicationOption) => {
            return Stream.die("Not implemented")

        }
    }


}