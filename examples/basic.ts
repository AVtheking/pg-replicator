import { Effect, Stream } from "effect"
import pg from "pg"
import * as PgReplicator from "../src"
import { NodeRuntime } from "@effect/platform-node"

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

    const repl = yield* PgReplicator.fromPg(connection)

    const startLSN = yield* repl.createReplicationSlot({ slotName, outputPlugin }).pipe(
        Effect.map((slot) => slot.consistentPoint),
        Effect.catchTag("SlotAlreadyExists", () =>
            Effect.logInfo(`slot ${slotName} already exists, resuming`).pipe(Effect.as(0n))
        )
    )

    yield* Effect.logInfo(`slot ${slotName} created at ${PgReplicator.formatLSN(startLSN)}`)

    yield* repl.startReplication({
        slot: slotName,
        startLSN: startLSN,
        publication: publicationName,
        protoVersion: 2,
    }).pipe(
        Stream.runForEach(PgReplicator.PgOutput.$match({
            Keepalive: (k) =>
                Effect.logInfo(`keepalive ${PgReplicator.formatLSN(k.serverWalEnd)} dated ${k.serverTime.toISOString()} replyRequested: ${k.replyRequested}`),
            Begin: (b) =>
                Effect.logInfo(`BEGIN ${b.finalLSN} ${b.commitTimestamp} ${b.xid}`),
            Relation: (r) => {
                const header = `| Name           | Data Type OID |\n| -------------- | ------------- |`
                const rows = r.relationColumns.map(c =>
                    `| ${c.name.padEnd(14)} | ${c.dataTypeOID.toString().padEnd(13)} |`
                )
                const table = [header, ...rows].join("\n")
                return Effect.logInfo(
                    [
                        `RELATION`,
                        `Relation ID: ${r.relationId}`,
                        `Namespace: ${r.namespace}`,
                        `Name: ${r.name}`,
                        `Replica Identity: ${r.replicaIdentity}`,
                        `Number of Columns: ${r.numberOfColumns}`,
                        table
                    ].join("\n")
                )
            },
            Insert: (i) => Effect.logInfo(`INSERT  ${JSON.stringify(i.rows)}`),
            Update: (u) => Effect.logInfo(`UPDATE  ${JSON.stringify(u.oldRows)} ${JSON.stringify(u.newRows)}`),
            Delete: (d) => Effect.logInfo(`DELETE  ${JSON.stringify(d.rows)}`),
            Commit: (c) => repl.ack(c.endLSN).pipe(
                Effect.andThen(Effect.logInfo(`Acknowledged commit with LSN ${PgReplicator.formatLSN(c.endLSN)} dated ${c.commitTimestamp}`)))
            ,
            Unknown: (u) =>
                Effect.logDebug(`unhandled message type ${u.type}`),
        }))
    )
})

RunReplication().pipe(
    Effect.scoped,
    NodeRuntime.runMain
)
