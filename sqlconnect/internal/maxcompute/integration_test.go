package maxcompute_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcsql"
)

// TestMaxComputeDB runs the shared database suite against the dev project. Its
// schemas are rs_retl_probe_sc_<run>_tsqlcon_<random>_<unix time>: never
// reused, dropped with CASCADE when the test ends, and swept by
// sqlconnect/cmd/cleanup if a run is killed.
func TestMaxComputeDB(t *testing.T) {
	configJSON := testConfig(t)
	prefix := objectName("")
	// The suite names its schemas formatfn("tsqlcon_…"); this run's prefix
	// marks them as ours, so the cleanup drops exactly them.
	formatfn := func(s string) string {
		s = mcsql.Normalise(s)
		if strings.HasPrefix(s, "tsqlcon_") {
			return prefix + s
		}
		return s
	}
	t.Cleanup(func() {
		db := testDB(t)
		ctx := context.Background()
		schemas, err := db.ListSchemas(ctx)
		if err != nil {
			t.Logf("listing schemas for cleanup: %v", err)
			return
		}
		for _, s := range schemas {
			if strings.HasPrefix(s.Name, prefix) {
				if err := db.DropSchema(ctx, s); err != nil {
					t.Logf("dropping schema %s: %v", s.Name, err)
				}
			}
		}
	})
	integrationtest.TestDatabaseScenarios(t, maxcompute.DatabaseType, configJSON, formatfn, integrationtest.Options{
		SpecialCharactersInQuotedTable: "_12", // MaxCompute names refuse - and .
		NoLegacyMappings:               true,
		ReportsViews:                   true,
		ExtraTests: func(t *testing.T, db sqlconnect.DB) {
			_, isController := db.(sqlconnect.QueryController)
			_, isClassifier := db.(sqlconnect.ErrorClassifier)
			require.True(t, isController && isClassifier, "the optional interfaces are implemented")
		},
	})
}
