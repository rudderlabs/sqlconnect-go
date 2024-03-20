package trino_test

import (
	"os"
	"strings"
	"testing"

	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/trino"
)

func TestTrinoDB(t *testing.T) {
	t.Setenv("TZ", "UTC") // set timezone to UTC for consistent datetime tests
	configJSON, ok := os.LookupEnv("TRINO_TEST_ENVIRONMENT_CREDENTIALS")
	if !ok {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatal("TRINO_TEST_ENVIRONMENT_CREDENTIALS environment variable not set")
		}
		t.Skip("skipping trino integration test due to lack of a test environment")
	}

	integrationtest.TestDatabaseScenarios(t, trino.DatabaseType, []byte(configJSON), strings.ToLower, integrationtest.Options{})

	integrationtest.TestSshTunnelScenarios(t, trino.DatabaseType, []byte(configJSON))
}
