import { Effect, Stream } from "effect"
import pg from "pg"
import * as PgRepl from "./PgRepl"
import { NodeRuntime, NodeSocket } from "@effect/platform-node"

const connectionString = "postgres://postgres:postgres@localhost:5434/syncengine?replication=database"
const slotName = "my_slot"
const publicationName = "sync_pub"
const outputPlugin = "pgoutput"


const RunReplication = Effect.fn(function* () {
    const connection = yield* Effect.promise(async () => {
        const connection = new pg.Client({
            connectionString,
        })
        await connection.connect()
        return connection
    })

    const repl = PgRepl.fromPg(connection)

    yield* repl.createReplicationSlot({ slotName, outputPlugin, options: { temporary: true } })

    yield* repl.startReplication({ slot: slotName, startLSN: "0/0", publication: publicationName, protoVersion: 3 }).pipe(
        Stream.runForEach((event) => {
            switch (event._tag) {
                case "Begin": return Effect.logInfo(`BEGIN xid=${event.xid}`)
                case "Insert": return Effect.logInfo(`INSERT ${event.relation.namespace}.${event.relation.name}`, event.row)
                case "Update": return Effect.logInfo(`UPDATE ${event.relation.name}`, event.old, event.new)
                case "Delete": return Effect.logInfo(`DELETE ${event.relation.name}`, event.old)
                case "Commit": return Effect.logInfo(`COMMIT lsn=${event.commitLsn}`)
                default: return Effect.logInfo(event._tag)
            }
        })
    )


})

const program = Effect.gen(function* () {
    yield* RunReplication()
})

program.pipe(
    Effect.scoped,
    Effect.provide(NodeSocket.layerNet({ host: "localhost", port: 5434 })),
    NodeRuntime.runMain
)