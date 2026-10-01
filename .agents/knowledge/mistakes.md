# Mistakes

> Post-mortem entries from observed failures: CI failures, reverts on prior PRs,
> prod incidents. Accrues over time — bootstrap leaves this empty.
> Append-only. Agent-authored sections may optionally carry an HTML-comment tag
> (e.g., `<!-- pr:<id> -->`) identifying the writer/PR/run; human-authored
> sections are conventionally left untouched by automated runs.

## ACT2-859 — Factory option and callback lint pitfalls

- CI's golangci-lint/staticcheck S1016 rejected a field-by-field copy from the internal DB options struct to the structurally identical exported factory options struct. Use direct conversion (`DBFactoryOptions(factoryOptions)`) while their layouts match; it preserves every field and satisfies the repository lint configuration (`sqlconnect/db_factory.go`).
- Factory callbacks in tests must not return `(nil, nil)`, even after `t.Fatal`, because the enabled `nilnil` linter analyzes the callback signature. Return a local sentinel error on the unreachable path while retaining the defensive test failure (`sqlconnect/db_factory_test.go`).

## ACT2-766 — Audience guard and toolchain mistakes

<!-- session: 2026-10-01 -->

- `clickhousequery/queryguard.go::CheckAudienceSQL` first refused table functions by inferred position: FROM and JOIN, parenthesised table lists, and a call used as an IN set. Each new shape was a bypass. For example, `SELECT uid FROM (url(...) AS a, db.users AS b)` passed and read a server file. Corrected model (owner decision D26): refuse the exact 26.3 table function names and the scalar `file()` in any position, bare or quoted, in any letter case. The structural classifier (`tableFunctionAt`, `wrappedTableFunction`, `inReadsTableFunction`) was deleted in 035a5c1. Do not add position rules back.
- A security finding against the guard was dismissed after one example query failed on the server. A later spelling of the same class read a file. A status note also said that a table function after IN does not run, and the first bypass disproved it. Corrected rule: before you reject a guard finding, try at least three other spellings of the same class on the pinned 26.3 server, with an admin user and with the customer user.
- The Go guard refused SQL that Lookout's audience compiler emits: keywords after a dot (`u.format`, `t.final`, `analytics.sample`), keywords before a dot (`FROM sample.users`, `by.users`), and one- or two-value lists `x IN (CAST(...), CAST(...))`. Each refusal would fail every sync of that audience. Fixed in 8a76a38, 81eb460 and 070830c. Corrected rule: run Lookout's compiled output through `CheckAudienceSQL` before you change a refusal, and add an `accept_` corpus row for each compiled form.
- Go 1.27 breaks this repository: the grpc dependency fails to build in tests, `go fix` in `make fmt` rewrites unrelated files, and golangci-lint crashes. Use `GOTOOLCHAIN=go1.26.0` for `go test`, `make fmt` and `make lint`.
- The image tag `clickhouse/clickhouse-server:26.3.33.24` now resolves to a different digest. `internal/clickhouse/chtest/images.json` pins 26.3, 25.8 and 24.8 by digest (26.3 is `sha256:810861a2e2d0188744f5f23b2d3ec9ff95812bcb9ddbb8fed13a377a7f305893`, owner decision D5). Do not refresh the pins from the tag.
