package bigquery_test

import (
	"os"
	"strings"
	"testing"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/bigquery"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
)

func TestBigqueryDB(t *testing.T) {
	configJSON, ok := os.LookupEnv("BIGQUERY_TEST_ENVIRONMENT_CREDENTIALS")
	if !ok {
		t.Skip("skipping bigquery integration test due to lack of a test environment")
	}
	integrationtest.TestDatabaseScenarios(t, bigquery.DatabaseType, []byte(configJSON), strings.ToLower, integrationtest.Options{LegacySupport: true})
}
