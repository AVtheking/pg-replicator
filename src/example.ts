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
                return Effect.forEach(chunk.tupleData.columns, (column) => {
                    switch (column.dataType) {
                        case "text":
                            return Effect.logInfo(`INSERT text ${chunk.relationId} ${column.value}`)
                        case "binary":
                            return Effect.logInfo(`INSERT binary ${chunk.relationId} ${Buffer.from(column.value).toString("utf-8")}`)
                        case "null":
                            return Effect.logInfo(`INSERT null ${chunk.relationId}`)
                        case "toast":
                            return Effect.logInfo(`INSERT toast ${chunk.relationId}`)
                    }
                }, { discard: true })
            }
            return Effect.void
        })
    )
})

RunReplication().pipe(
    Effect.scoped,
    NodeRuntime.runMain
)
