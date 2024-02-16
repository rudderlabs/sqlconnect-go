package redshift_test

import (
	"os"
	"strings"
	"testing"

	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/redshift"
)

func TestRedshiftDB(t *testing.T) {
	configJSON, ok := os.LookupEnv("REDSHIFT_TEST_ENVIRONMENT_CREDENTIALS")
	if !ok {
		t.Skip("skipping redshift integration test due to lack of a test environment")
	}

	integrationtest.TestDatabaseScenarios(t, redshift.DatabaseType, []byte(configJSON), strings.ToLower, integrationtest.Options{LegacySupport: true})
}
