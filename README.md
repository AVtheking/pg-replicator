# pg-replicator

Effect-native PostgreSQL logical replication client built on [node-postgres](https://node-postgres.com/).

Consume your database's write-ahead log as an Effect `Stream` of typed `Begin` / `Insert` / `Update` / `Delete` / `Commit` messages, decoded from the built-in `pgoutput` plugin. Useful for change data capture (CDC), cache invalidation, search indexing, syncing to another store, or anything else that needs to react to row changes without polling.

> **Status:** early (`0.x`). The API may change between minor versions. Built against `effect@4.0.0-rc`.

## Features

- `Stream.Stream<PgOutput, PgReplError>` over a replication connection
- Decodes `pgoutput` protocol messages: Begin, Commit, Relation, Insert, Update, Delete
- Tracks `Relation` messages so Insert/Update/Delete arrive with column-named `rows` objects
- Decodes common Postgres text types (`bool`, `int2/4/8`, `float4/8`, `timestamp[tz]`, `json[b]`) to JS values
- Create / drop replication slots, resume from an LSN, acknowledge progress with `ack`
- Replies to server keepalives automatically
- `bigint` LSNs with `parseLSN` / `formatLSN` helpers

## Install

```sh
pnpm add pg-replicator effect pg
# or
npm install pg-replicator effect pg
```

`effect` and `pg` are peer dependencies. If you use TypeScript you will also want `@types/pg`.

## Postgres setup

Logical replication has to be enabled on the server and a publication has to exist for the tables you care about.

```sql
-- postgresql.conf (restart required)
wal_level = logical
max_replication_slots = 4     -- at least 1
max_wal_senders = 4           -- at least 1

-- as a superuser or a role with REPLICATION privilege
CREATE PUBLICATION my_pub FOR TABLE users, orders;
-- or: CREATE PUBLICATION my_pub FOR ALL TABLES;
```

For `UPDATE` / `DELETE` to include the old row, set the table's replica identity:

```sql
ALTER TABLE users REPLICA IDENTITY FULL;      -- send the whole old row
-- or the default, which sends only the primary key
```

## Usage

```ts
import { Effect, Stream } from "effect"
import { NodeRuntime } from "@effect/platform-node"
import pg from "pg"
import * as PgReplicator from "pg-replicator"

const program = Effect.gen(function* () {
  // The connection must be opened in replication mode.
  const client = yield* Effect.acquireRelease(
    Effect.promise(async () => {
      const c = new pg.Client({
        connectionString: "postgres://postgres:postgres@localhost:5432/mydb",
        replication: "database",
      } as pg.ClientConfig)
      await c.connect()
      return c
    }),
    (c) => Effect.promise(() => c.end()),
  )

  const repl = yield* PgReplicator.fromPg(client)

  // Create the slot on first run; resume from the beginning of the slot on later runs.
  const startLSN = yield* repl
    .createReplicationSlot({ slotName: "my_slot", outputPlugin: "pgoutput" })
    .pipe(
      Effect.map((slot) => slot.consistentPoint),
      Effect.catchTag("SlotAlreadyExists", () => Effect.succeed(0n)),
    )

  yield* repl
    .startReplication({
      slot: "my_slot",
      startLSN,
      publication: "my_pub",
      protoVersion: 2,
    })
    .pipe(
      Stream.runForEach(
        PgReplicator.PgOutput.$match({
          Begin: (b) => Effect.logInfo(`BEGIN xid=${b.xid}`),
          Insert: (i) => Effect.logInfo(`INSERT ${JSON.stringify(i.rows)}`),
          Update: (u) => Effect.logInfo(`UPDATE ${JSON.stringify(u.oldRows)} -> ${JSON.stringify(u.newRows)}`),
          Delete: (d) => Effect.logInfo(`DELETE ${JSON.stringify(d.rows)}`),
          // Acknowledge after each transaction so Postgres can recycle WAL.
          Commit: (c) => repl.ack(c.endLSN),
          Relation: () => Effect.void,
          Keepalive: () => Effect.void,
          Unknown: (u) => Effect.logDebug(`unhandled message ${u.type}`),
        }),
      ),
    )
})

program.pipe(Effect.scoped, NodeRuntime.runMain)
```

A runnable version lives in [`examples/basic.ts`](./examples/basic.ts).

## API

### `fromPg(client: pg.Client): Effect<PgReplicator>`

Wraps an already-connected `pg.Client` that was created with `replication: "database"`. Returns a `PgReplicator`.

### `PgReplicator`

```ts
interface PgReplicator {
  createReplicationSlot(option: CreateReplicationSlot): Effect<CreateReplicationSlotResult, PgReplError | SlotAlreadyExists>
  dropReplicationSlot(slotName: string): Effect<void, PgReplError>
  startReplication(option: StartReplicationOption): Stream<PgOutput, PgReplError>
  ack(lsn: bigint): Effect<void, PgReplError>
}
```

- **`createReplicationSlot`** runs `CREATE_REPLICATION_SLOT`. Pass `options.temporary: true` for a slot that disappears when the connection closes. Fails with `SlotAlreadyExists` (tag `"SlotAlreadyExists"`) if the name is taken, which is the normal signal to resume instead.
- **`startReplication`** runs `START_REPLICATION` and returns a stream of decoded messages. `startLSN` is a `bigint`; use `0n` to let the server start from the slot's confirmed position, or the `consistentPoint` returned by `createReplicationSlot`. `protoVersion` should be `2` for Postgres 14+ (`1` for older servers). Listeners and the COPY stream are cleaned up when the stream's scope closes.
- **`ack`** sends a standby status update telling the server everything up to `lsn` has been processed. Call it with `Commit.endLSN` once you've durably handled a transaction. Until you ack, Postgres retains WAL for the slot, so a consumer that never acks will fill the server's disk.
- **`dropReplicationSlot`** runs `DROP_REPLICATION_SLOT`.

### `PgOutput`

A `Data.TaggedEnum` with a `$match` helper. Variants:

| Tag | Fields | Notes |
|---|---|---|
| `Begin` | `finalLSN: bigint`, `commitTimestamp: bigint`, `xid: number` | Start of a transaction |
| `Commit` | `flags: number`, `commitLSN: bigint`, `endLSN: bigint`, `commitTimestamp: Date` | Ack with `endLSN` |
| `Relation` | `relationId`, `namespace`, `name`, `replicaIdentity`, `numberOfColumns`, `relationColumns` | Table schema; sent before the first change to each table and after DDL |
| `Insert` | `relationId`, `tupleData`, `rows?` | `rows` is `Record<columnName, value>` |
| `Update` | `relationId`, `oldTupleKind?`, `oldTupleData?`, `newTupleData`, `oldRows?`, `newRows?` | `oldRows` present only when replica identity is `FULL` or the key changed |
| `Delete` | `relationId`, `oldTupleKind?`, `oldTupleData?`, `rows?` | `rows` holds the key or the full old row depending on replica identity |
| `Keepalive` | `serverWalEnd: bigint`, `serverTime: Date`, `replyRequested: boolean` | Replies are sent for you |
| `Unknown` | `type: string`, `walData: Buffer` | Message types not decoded yet (Truncate, Type, Origin, Message, streaming) |

`rows` are decoded from Postgres text values using `defaultDecoders` (keyed by type OID); unknown types are left as strings, `NULL` becomes `null`, unchanged TOAST values become `"(unchanged)"`.

### LSN helpers

```ts
parseLSN("0/16B3748")   // 23803720n
formatLSN(23803720n)    // "0/16B3748"
```

### Errors

- `PgReplError` – `{ message: string; cause?: unknown }`, tag `"PgReplError"`
- `SlotAlreadyExists` – `{ slotName: string }`, tag `"SlotAlreadyExists"`

Both are `Data.TaggedError`s and work with `Effect.catchTag`.

## Not yet supported

- `TRUNCATE`, `TYPE`, `ORIGIN`, `MESSAGE` messages (delivered as `Unknown`)
- Streaming of in-progress transactions (`proto_version` 2+ with `streaming 'on'`)
- Custom column decoders (`defaultDecoders` is used internally; not yet configurable)
- Physical replication

## Development

```sh
pnpm install
pnpm typecheck        # tsc --noEmit over src/ and examples/
pnpm build            # tsdown -> dist/
pnpm example          # bun examples/basic.ts (needs a local Postgres, see the file for the connection string)
```

## License

[ISC](./LICENSE)
