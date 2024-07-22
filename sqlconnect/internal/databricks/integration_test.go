package databricks_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/databricks"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
)

func TestDatabricksDB(t *testing.T) {
	configJSON, ok := os.LookupEnv("DATABRICKS_TEST_ENVIRONMENT_CREDENTIALS")
	if !ok {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatal("DATABRICKS_TEST_ENVIRONMENT_CREDENTIALS environment variable not set")
		}
		t.Skip("skipping databricks integration test due to lack of a test environment")
	}

	t.Run("with information schema", func(t *testing.T) {
		configJSON, err := sjson.Set(configJSON, "retryAttempts", 4)
		require.NoError(t, err, "failed to set retryAttempts")
		configJSON, err = sjson.Set(configJSON, "minRetryWaitTime", time.Second)
		require.NoError(t, err, "failed to set minRetryWaitTime")
		configJSON, err = sjson.Set(configJSON, "maxRetryWaitTime", 30*time.Second)
		require.NoError(t, err, "failed to set maxRetryWaitTime")
		configJSON, err = sjson.Set(configJSON, "catalog", "sqlconnect")
		require.NoError(t, err, "failed to set catalog")
		db, err := sqlconnect.NewDB(databricks.DatabaseType, []byte(configJSON))
		require.NoError(t, err, "failed to create db")
		_, err = db.Exec("SELECT * FROM INFORMATION_SCHEMA.COLUMNS LIMIT 1")
		require.NoError(t, err, "information schema should be available")

		integrationtest.TestDatabaseScenarios(
			t,
			databricks.DatabaseType,
			[]byte(configJSON),
			strings.ToLower,
			integrationtest.Options{
				LegacySupport:                  true,
				SpecialCharactersInQuotedTable: "`-",
			},
		)
	})

	t.Run("without information schema", func(t *testing.T) {
		configJSON, err := sjson.Set(configJSON, "catalog", "hive_metastore")
		require.NoError(t, err, "failed to set catalog")
		db, err := sqlconnect.NewDB(databricks.DatabaseType, []byte(configJSON))
		require.NoError(t, err, "failed to create db")
		_, err = db.Exec("SELECT * FROM INFORMATION_SCHEMA.COLUMNS LIMIT 1")
		require.Error(t, err, "information schema should not be available")
		require.ErrorContains(t, err, "TABLE_OR_VIEW_NOT_FOUND", "information schema should not be available")

		integrationtest.TestDatabaseScenarios(
			t,
			databricks.DatabaseType,
			[]byte(configJSON),
			strings.ToLower,
			integrationtest.Options{
				LegacySupport:                  true,
				SpecialCharactersInQuotedTable: "_A", // No special characters allowed
			},
		)

		integrationtest.TestSshTunnelScenarios(t, databricks.DatabaseType, []byte(configJSON))
	})
}
