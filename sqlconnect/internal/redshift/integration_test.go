package redshift_test

import (
	"os"
	"strings"
	"testing"

	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/redshift"
)

func TestRedshiftDB(t *testing.T) {
	t.Run("postgres driver", func(t *testing.T) {
		configJSON, ok := os.LookupEnv("REDSHIFT_TEST_ENVIRONMENT_CREDENTIALS")
		if !ok {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatal("REDSHIFT_TEST_ENVIRONMENT_CREDENTIALS environment variable not set")
			}
			t.Skip("skipping redshift postgres driver integration test due to lack of a test environment")
		}

		integrationtest.TestDatabaseScenarios(t, redshift.DatabaseType, []byte(configJSON), strings.ToLower, integrationtest.Options{LegacySupport: true})

		integrationtest.TestSshTunnelScenarios(t, redshift.DatabaseType, []byte(configJSON))
	})

	t.Run("redshift data driver", func(t *testing.T) {
		configJSON, ok := os.LookupEnv("REDSHIFT_DATA_TEST_ENVIRONMENT_CREDENTIALS")
		if !ok {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatal("REDSHIFT_DATA_TEST_ENVIRONMENT_CREDENTIALS environment variable not set")
			}
			t.Skip("skipping redshift data driver integration test due to lack of a test environment")
		}
		integrationtest.TestDatabaseScenarios(t, redshift.DatabaseType, []byte(configJSON), strings.ToLower, integrationtest.Options{LegacySupport: true})
	})
}
