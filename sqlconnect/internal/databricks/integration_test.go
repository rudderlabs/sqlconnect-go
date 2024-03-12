package databricks_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/databricks"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
)

func TestDatabricksDB(t *testing.T) {
	configJSON, ok := os.LookupEnv("DATABRICKS_TEST_ENVIRONMENT_CREDENTIALS")
	if !ok {
		t.Skip("skipping databricks integration test due to lack of a test environment")
	}

	configJSON, err := sjson.Set(configJSON, "retryAttempts", 4)
	require.NoError(t, err, "failed to set retryAttempts")
	configJSON, err = sjson.Set(configJSON, "minRetryWaitTime", time.Second)
	require.NoError(t, err, "failed to set minRetryWaitTime")
	configJSON, err = sjson.Set(configJSON, "maxRetryWaitTime", 30*time.Second)
	require.NoError(t, err, "failed to set maxRetryWaitTime")

	integrationtest.TestDatabaseScenarios(t, databricks.DatabaseType, []byte(configJSON), strings.ToLower, integrationtest.Options{LegacySupport: true})
}
