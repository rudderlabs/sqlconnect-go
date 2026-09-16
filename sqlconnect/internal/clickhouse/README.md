# ClickHouse tracer-bullet driver

Import `sqlconnect/config` to register the `clickhouse` DB and dialect factories.
`config.ClickHouse` exposes the eight account fields: host, port, database, user,
password, secure, skipVerify, and scratchDatabase. Native TLS is enabled by default
on port 9440. The mini fixture can explicitly set skipVerify=true. Password bytes
are preserved, including an empty password. Unknown account fields are rejected.

A ClickHouse database is a sqlconnect schema. Identifiers preserve case, and SQL
uses backtick quoting. Catalog operations and transaction/prepare APIs are
unsupported. Ping checks version() against 25.8.0, verifies currentDatabase(),
and reads the relevant system.settings on one connection.

Metadata uses bound values against system.databases, system.tables, and
system.columns. Query metadata executes a SELECT wrapper with LIMIT 0 and reads
ColumnTypes. Native declarations remain in ColumnRef.RawType. UInt64, larger
integers, and decimals export as strings; unsupported composite types require an
explicit projection. The mapper rejects conversion errors instead of emitting a
partial row.

Use per-query settings on calls through the sqlconnect DB:

```go
ctx = sqlconnect.WithQuerySettings(ctx, map[string]any{
    "final": 1,
    "skip_unavailable_shards": 0,
    "max_replica_delay_for_distributed_queries": 1,
    "fallback_to_stale_replicas_for_distributed_queries": 0,
})
rows, err := db.QueryContext(ctx, query)
```

For this helper, the adapter builds a complete map with
clickhouse.Context/WithSettings for each query. It enforces join_use_nulls=1, async_insert=0, wait_for_async_insert=1, and
select_sequential_consistency=1. This helper replaces parent settings and is
consumed by the adapter's query/exec methods. Raw SqlDB() or Conn() access bypasses
that helper; those callers must attach the native driver's context settings.
Contexts supplied directly with the native clickhouse.Context/WithSettings API
are preserved when the sqlconnect helper is absent. Those callers own the full
settings map and must retain the correctness settings above.
Use the context deadline for the query budget.

## Caller-visible limitations

- CreateTableFromQuery executes CREATE with explicit native columns and
  `ENGINE = MergeTree ORDER BY tuple()`, followed by INSERT ... SELECT. The empty
  sorting key provides no key-based pruning or ordering guarantee. Callers that
  require a sorting key must create their table explicitly. The broader LLD's
  MaterializationAdmin/options interface is outside this tracer bullet.
- CREATE and INSERT are not transactional. An INSERT failure can leave a partial
  target. The caller must reconcile it and use a fresh attempt name; the driver
  does not retry or publish it automatically.
- MoveTable uses RENAME TABLE and requires an absent destination. Replacing an
  existing snapshot requires an explicit EXCHANGE TABLES from the source client.
  The driver does not implement copy-then-drop moves.
- Managed table DDL writes only to the configured, pre-created scratchDatabase.
  Database creation/deletion is unsupported. Arbitrary SQL submitted through Exec
  remains the caller's responsibility; server grants enforce the write boundary.
- No SSH, cluster, Cloud, grant-graph inspection, or live database verification is
  included in this package's tracer-bullet scope. Source validation must still
  check scratch existence and create/drop its permission table.

The dependency is pinned to github.com/rudderlabs/clickhouse-go/v2 v2.48.0.
Dependency resolution and live fixture tests must pass before this driver ships.
