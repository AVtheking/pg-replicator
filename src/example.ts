import { Effect, Stream } from "effect"
import pg from "pg"
import * as PgRepl from "./PgRepl"
import { NodeRuntime } from "@effect/platform-node"

const connectionString = "postgres://postgres:postgres@localhost:5434/syncengine"
const slotName = "my_slot"
const publicationName = "sync_pub"
const outputPlugin = "pgoutput"


const RunReplication = Effect.fn(function* () {
    // replication: "database" is what turns this into a walsender connection.
    // @types/pg doesn't declare the field, but the pg runtime reads it.
    const connection = yield* Effect.acquireRelease(
        Effect.promise(async () => {
            const connection = new pg.Client({
                connectionString,
                replication: "database",
            } as pg.ClientConfig)
            await connection.connect()
            return connection
        }),
        (connection) => Effect.promise(() => connection.end())
    )

    const repl = PgRepl.fromPg(connection)

    const slot = yield* repl.createReplicationSlot({ slotName, outputPlugin, options: { temporary: true } })
    yield* Effect.logInfo(`slot ${slot.name} created at ${slot.consistentPoint}`)

    yield* repl.startReplication({
        slot: slot.name,
        startLSN: slot.consistentPoint,
        publication: publicationName,
        protoVersion: 2,
    }).pipe(
        // Raw CopyData payloads for now: first byte is 'w' (XLogData) or 'k' (keepalive).
        Stream.runForEach((chunk) => {
            if (chunk._tag === "keepalive") {
                return Effect.logInfo(`keepalive ${chunk.serverWalEnd} ${chunk.serverTime} ${chunk.replyRequested}`)
            }
            if (chunk._tag === "BEGIN") {
                return Effect.logInfo(`BEGIN ${chunk.finalLSN} ${chunk.commitTimestamp} ${chunk.xid}`)
            }
            if (chunk._tag === "INSERT") {
                return Effect.logInfo(`INSERT ${chunk.relationId} ${chunk.tupleData.numberOfColumns} columns ${chunk.tupleData.columns.map((column) => `${column.dataType} ${column.length} bytes`).join(", ")}`)
            }
            return Effect.void
        })
    )
})

RunReplication().pipe(
    Effect.scoped,
    NodeRuntime.runMain
)
