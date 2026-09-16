import { Effect, Stream } from "effect"
import pg from "pg"
import * as PgRepl from "./PgRepl"
import { NodeRuntime } from "@effect/platform-node"
import { Column, PgOutput } from "./PgRepl"

const connectionString = "postgres://postgres:postgres@localhost:5434/syncengine"
const slotName = "my_slot"
const publicationName = "sync_pub"
const outputPlugin = "pgoutput"


const RunReplication = Effect.fn(function* () {

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
        Stream.runForEach(PgOutput.$match({
            Keepalive: (k) =>
                Effect.logInfo(`keepalive ${k.serverWalEnd} ${k.serverTime} ${k.replyRequested}`),
            Begin: (b) =>
                Effect.logInfo(`BEGIN ${b.finalLSN} ${b.commitTimestamp} ${b.xid}`),
            Relation: (r) =>
                Effect.logInfo(`RELATION ${r.relationId} ${r.namespace} ${r.name} ${r.replicaIdentity} ${r.numberOfColumns}` + r.relationColumns.map(c => ` ${c.name} ${c.dataTypeOID} ${c.dataTypeModifier}`).join("")),
            Insert: (i) =>
                Effect.forEach(i.tupleData.columns, Column.$match({
                    Null: () => Effect.logInfo(`INSERT null ${i.relationId}`),
                    Toast: () => Effect.logInfo(`INSERT toast ${i.relationId}`),
                    Text: (c) => Effect.logInfo(`INSERT text ${i.relationId} ${c.value}`),
                    Binary: (c) => Effect.logInfo(`INSERT binary ${i.relationId} ${Buffer.from(c.value).toString("utf-8")}`),
                }), { discard: true }),
            Unknown: (u) =>
                Effect.logDebug(`unhandled message type ${u.type}`),
        }))
    )
})

RunReplication().pipe(
    Effect.scoped,
    NodeRuntime.runMain
)
