# Mistakes

> Post-mortem entries from observed failures: CI failures, reverts on prior PRs,
> prod incidents. Accrues over time — bootstrap leaves this empty.
> Append-only. Agent-authored sections may optionally carry an HTML-comment tag
> (e.g., `<!-- pr:<id> -->`) identifying the writer/PR/run; human-authored
> sections are conventionally left untouched by automated runs.

## ACT2-859 — Prefer direct conversion for matching option structs

- CI's golangci-lint/staticcheck S1016 rejected a field-by-field copy from the internal DB options struct to the structurally identical exported factory options struct. Use direct conversion (`DBFactoryOptions(factoryOptions)`) while their layouts match; it preserves every field and satisfies the repository lint configuration (`sqlconnect/db_factory.go`).
