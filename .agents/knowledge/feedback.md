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

- D26 (Leo, 2026-10-01): the ClickHouse connector trusts the credential, as the other warehouses do. `clickhousequery.CheckAudienceSQL` is defence in depth, not a sandbox. It keeps the token-aware size limit (`MaxAudienceSQLBytes`, 240 KiB), the statement count, normalisation, the refused audience clauses, and the exact name refusal of the 26.3 table functions plus the scalar `file()`. It does not see what views, dictionaries, executable UDFs or engine-backed tables read, and it does not refuse table functions added after 26.3. D26 reverses D24, which refused any lone call used as an IN set.
- D41 and D42: `arrayJoin()` stays accepted by the guard as a known limit. A final semicolon followed by a comment is refused.
- The driver allows private network ranges (RFC 1918, ULA, CGNAT), because PrivateLink warehouses resolve to private addresses. The owner rejected a default block list and extra refusals for the IMDS /64, `::/96` and `0.0.0.0/8`. Operators add refusals through `clickhousequery.ParseBlockedCIDRs` and `SetDialPolicy`. The production refusal of private addresses (D20, D27) lives in Lookout only, not in this repository.
- The account config has exactly eight fields. `secure` must be true, `skipVerify` must be false, and unknown keys fail. Plain HTTP and loopback are reachable only through `internal/chpolicy.Policy.AllowPlainHTTP` and `AllowLoopback`, which only test code sets (D3). Production code must never set them.
- Owner rule "keep the scope tight, simple and elegant" (2026-09-30): parity with the other warehouses wins ties, and speculative hardening goes. The source hides behind a feature flag, so rollback is turning the flag off. For this reason `max_memory_usage` was dropped from `driverReadSettings`: a customer settings-profile constraint on it would fail validation stage 2.
- D17: `MaxRunBudget` is 2 h. The DDL visibility wait in `internal/clickhouse/visibility.go` starts at 250 ms, doubles up to 5 s, and stops after 60 s total. Keep these values unless the spec changes.
