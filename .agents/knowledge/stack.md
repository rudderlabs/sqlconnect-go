# Stack

> Dependencies, frameworks, tooling.
> Append-only. Agent-authored sections may optionally carry an HTML-comment tag
> (e.g., `<!-- pr:<id> -->`) identifying the writer/PR/run; human-authored
> sections are conventionally left untouched by automated runs.

## Language and Toolchain Baseline
<!-- ticket:RUD-2789 -->

- Module: `github.com/rudderlabs/sqlconnect-go` (`go.mod`).
- Language version: `go 1.26.0` (`go.mod`), aligned with CI setup and lint config (`.github/workflows/test.yaml`, `.github/workflows/verify.yml`, `.golangci.yml`).
- Primary build/test task runner: `Makefile` targets (`test`, `generate`, `fmt`, `lint`) with install-on-demand tool bootstrap.

## Runtime and Driver Dependencies
<!-- ticket:RUD-2789 -->

- Warehouse drivers: `cloud.google.com/go/bigquery`, `github.com/databricks/databricks-sql-go`, `github.com/go-sql-driver/mysql`, `github.com/lib/pq`, `github.com/snowflakedb/gosnowflake`, `github.com/trinodb/trino-go-client` (`go.mod`).
- AWS/Redshift data path: `github.com/aws/aws-sdk-go-v2` family plus `service/redshiftdata` (`go.mod`).
- SQL abstraction and expression building: `github.com/rudderlabs/goqu/v10` (`go.mod`, `sqlconnect/db.go`).
- JSON config/value helpers heavily used at boundaries: `github.com/tidwall/gjson` and `github.com/tidwall/sjson` (`go.mod`, `sqlconnect/internal/redshift/db.go`, `sqlconnect/cmd/cleanup/cleanup.go`).

## Tooling, Formatting, and Linting
<!-- ticket:RUD-2789 -->

- Formatting path is deterministic and includes `go fix`, `gofumpt`, and `gci` import ordering (`Makefile::fmt`).
- Linting uses `golangci-lint` v2 with explicit linter set and Go 1.26 runtime (`Makefile::lint`, `.golangci.yml`).
- Test execution uses `gotestsum` with rerun-fails, shuffle, coverage, and vet enabled by default (`Makefile::test-run`).

## CI and Release Automation
<!-- ticket:RUD-2789 -->

- `test.yaml`: matrix test + integration env + merged coverage upload (`.github/workflows/test.yaml`).
- `verify.yml`: enforces `go mod tidy`, generated artifacts, formatting, and lint checks (`.github/workflows/verify.yml`).
- `cleanup-test-schemas.yaml`: scheduled/workflow-dispatch cleanup command over test environments (`.github/workflows/cleanup-test-schemas.yaml`).
- `release-please.yaml`: automated release PR/tag flow currently configured with `release-type: go` and `package-name: rudder-server` (`.github/workflows/release-please.yaml`).
