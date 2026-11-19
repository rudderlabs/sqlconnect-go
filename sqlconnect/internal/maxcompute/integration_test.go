package maxcompute_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcsql"
)

// TestMaxComputeDB runs the shared database suite against the dev project. Its
// schemas are tsqlcon_<random>_<unix time>: never reused, dropped with CASCADE
// at the end, and swept by sqlconnect/cmd/cleanup.
func TestMaxComputeDB(t *testing.T) {
	configJSON := testConfig(t)
	integrationtest.TestDatabaseScenarios(t, maxcompute.DatabaseType, configJSON, mcsql.Normalise, integrationtest.Options{
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
