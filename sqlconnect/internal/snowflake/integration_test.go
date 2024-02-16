package snowflake_test

import (
	"os"
	"strings"
	"testing"

	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/snowflake"
)

func TestSnowflakeDB(t *testing.T) {
	configJSON, ok := os.LookupEnv("SNOWFLAKE_TEST_ENVIRONMENT_CREDENTIALS")
	if !ok {
		t.Skip("skipping snowflake integration test due to lack of a test environment")
	}

	integrationtest.TestDatabaseScenarios(t, snowflake.DatabaseType, []byte(configJSON), strings.ToUpper, integrationtest.Options{LegacySupport: true})
}
