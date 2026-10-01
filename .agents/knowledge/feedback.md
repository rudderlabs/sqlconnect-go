# Feedback

> Durable human direction and review guidance.
> Append-only. Agent-authored sections may optionally carry an HTML-comment tag
> identifying the writer/PR/run; human-authored sections are conventionally left
> untouched by automated runs.

## ACT2-859 — BigQuery token-source API safeguards

<!-- session: 2026-09-25 -->

- Keep `sqlconnect.WithBigQueryTokenSource` token-source-only and bind it to the
  `bigquery` factory; arbitrary Google client options could discard a customer's
  configured key and silently fall back to the pod identity, while use with another
  option-aware factory would otherwise be silently ignored.
- `sqlconnect.NewDB` must reject nil `DBOption` values, and
  `WithBigQueryTokenSource` must reject both nil and typed-nil token sources before
  factory construction so malformed inputs return errors rather than panic on query.
- Regression tests for `sqlconnect/internal/bigquery/db.go::init` must call the public
  `sqlconnect.NewDB("bigquery", ...)` path and force a query with a sentinel-error
  token source; constructor-only tests can pass when registration accidentally drops
  the token source and Google authentication falls back to ambient credentials.

## ACT2-766 — Owner decisions for the ClickHouse driver

<!-- session: 2026-10-01 -->

- The ClickHouse connector trusts the credential, as the other warehouses do. `CheckAudienceSQL` in `sqlconnect/clickhousequery/queryguard.go` is defence in depth, not a sandbox. It keeps the token-aware size limit (`MaxAudienceSQLBytes`, 240 KiB), the statement count, normalisation, the refused audience clauses, and the exact name refusal of the 26.3 table functions plus the scalar `file()`. It does not see what views, dictionaries, executable UDFs or engine-backed tables read, and it does not refuse table functions added after 26.3. A lone call used as an IN set is not refused by position.
- `CheckAudienceSQL` accepts `arrayJoin()` as a known limit. It refuses a terminal semicolon followed by a comment. `sqlconnect/clickhousequery/queryguard_test.go` covers both.
- The driver allows private network ranges (RFC 1918, ULA, CGNAT), because PrivateLink warehouses resolve to private addresses. The default policy in `sqlconnect/internal/chpolicy/chpolicy.go` has no block list, and adds no refusals for the IMDS /64, `::/96` or `0.0.0.0/8`. Operators add refusals through `clickhousequery.ParseBlockedCIDRs` and `clickhousequery.SetDialPolicy`.
- The account config accepts eight keys (`accountKeys` in `sqlconnect/internal/clickhouse/config.go`). `port` is optional and defaults to `DefaultPort`. `secure` is required and must be true, `skipVerify` must be false, and unknown keys fail. Plain HTTP and loopback are reachable only through `chpolicy.Policy.AllowPlainHTTP` and `AllowLoopback` in `sqlconnect/internal/chpolicy/chpolicy.go`, which only test code sets. Production code must never set them.
- Keep the scope tight, simple and elegant: parity with the other warehouses wins ties, and speculative hardening goes. For this reason `max_memory_usage` is not in `driverReadSettings` (`sqlconnect/internal/clickhouse/settings.go`): a customer settings-profile constraint on it would fail validation stage 2. Do not add read settings that a customer profile can conflict with.
- `MaxRunBudget` in `sqlconnect/clickhousequery/queryid.go` is 2 h. The DDL visibility wait in `sqlconnect/internal/clickhouse/visibility.go` starts at 250 ms, doubles up to 5 s, and stops after 60 s total (`defaultInitialBackoff`, `defaultMaxBackoff`, `defaultDeadline`). `sqlconnect.VisibilityPolicy` can override these defaults. Keep the values unless the spec changes.
