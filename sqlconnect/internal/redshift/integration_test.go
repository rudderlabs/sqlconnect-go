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
			t.Skip("skipping redshift pg integration test due to lack of a test environment")
		}

		integrationtest.TestDatabaseScenarios(t, redshift.DatabaseType, []byte(configJSON), strings.ToLower, integrationtest.Options{LegacySupport: true})
	})

	t.Run("sdk driver", func(t *testing.T) {
		configJSON, ok := os.LookupEnv("REDSHIFT_SDK_TEST_ENVIRONMENT_CREDENTIALS")
		if !ok {
			t.Skip("skipping redshift sdk integration test due to lack of a test environment")
		}
		integrationtest.TestDatabaseScenarios(t, redshift.DatabaseType, []byte(configJSON), strings.ToLower, integrationtest.Options{LegacySupport: true})
	})
}
