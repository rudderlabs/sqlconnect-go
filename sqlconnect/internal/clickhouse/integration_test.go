package clickhouse_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse/chtest"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
)

// TestClickHouseDB runs the shared database suite over HTTPS and plain HTTP.
func TestClickHouseDB(t *testing.T) {
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	for mode, secure := range map[string]bool{"https": true, "http": false} {
		t.Run(mode, func(t *testing.T) {
			integrationtest.TestDatabaseScenarios(t, clickhouse.DatabaseType,
				srv.Config(srv.AdminUser, srv.AdminPassword, "default", secure),
				func(s string) string { return s },
				integrationtest.Options{
					CreateTableSuffix:          " ENGINE = MergeTree ORDER BY tuple()",
					QuotedConditionIdentifiers: true,
					NoLegacyMappings:           true,
					OpaqueExpressionErrors:     true,
					ReportsViews:               true,
					NewDB: func(cfg json.RawMessage) (sqlconnect.DB, error) {
						return clickhouse.NewDBForTest(cfg, chpolicy.Policy{AllowLoopback: true, AllowPlainHTTP: true}, srv.CA)
					},
					ExtraTests: func(t *testing.T, db sqlconnect.DB) {
						require.True(t, is[sqlconnect.ValidationReporter](db) && is[sqlconnect.MaterializationAdmin](db) && is[sqlconnect.QueryCanceller](db) &&
							is[sqlconnect.VisibilityChecker](db) && is[sqlconnect.ErrorClassifier](db), "the optional interfaces are implemented")
					},
				})
		})
	}
}

func is[T any](v any) bool { _, ok := v.(T); return ok }
