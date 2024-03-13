package databricks_test

import (
	"os"
	"strings"
	"testing"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/databricks"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
)

func TestDatabricksDB(t *testing.T) {
	configJSON, ok := os.LookupEnv("DATABRICKS_TEST_ENVIRONMENT_CREDENTIALS")
	if !ok {
		t.Skip("skipping databricks integration test due to lack of a test environment")
	}

	integrationtest.TestDatabaseScenarios(t, databricks.DatabaseType, []byte(configJSON), strings.ToLower, integrationtest.Options{LegacySupport: true})
}
