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

- `CheckAudienceSQL` in `sqlconnect/clickhousequery/queryguard.go` first refused table functions by inferred position: FROM and JOIN, parenthesised table lists, and a call used as an IN set. Each new shape was a bypass. For example, `SELECT uid FROM (url(...) AS a, db.users AS b)` passed the position rules, and the `url` table function can read from a remote source. Corrected model: refuse the exact 26.3 table function names and the scalar `file()` in any position, bare or quoted, in any letter case. The structural classifier (`tableFunctionAt`, `wrappedTableFunction`, `inReadsTableFunction`) was deleted. Do not add position rules back.
- Do not dismiss a guard finding after one example query fails on the server. A later spelling of the same class can still read a file, and the ClickHouse parser maps several spellings to one tree. Before you reject a finding, try at least three other spellings of the same class on the pinned 26.3 server, with an admin user and with the customer user. `TestQueryGuard_TableFunctionForms` in `sqlconnect/clickhousequery/queryguard_test.go` holds the spelling cases.
- The guard must accept the SQL that an audience compiler emits. Accepted forms: keywords after a dot (`u.format`, `t.final`, `analytics.sample`), keywords before a dot (`FROM sample.users`, `by.users`), and one- or two-value lists `x IN (CAST(...), CAST(...))`. A wrongly refused form fails every sync of that audience. Before you change a refusal, run representative compiled output through `CheckAudienceSQL`, and add an `accept_` row to `sqlconnect/clickhousequery/testdata/audience-sql.json` for each form.
