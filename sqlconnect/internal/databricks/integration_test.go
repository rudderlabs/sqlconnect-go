package databricks_test

import (
	"context"
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

var extraTests = func(t *testing.T, db sqlconnect.DB) {
	ctx := context.Background()

	t.Run("schema admin", func(t *testing.T) {
		t.Run("exists", func(t *testing.T) {
			t.Run("with nonexistent catalog", func(t *testing.T) {
				_, err := db.SchemaExists(ctx, sqlconnect.SchemaRef{Name: "test_schema"}, sqlconnect.WithCatalog("nonexistent"))
				require.Error(t, err, "it should not be able to check if a schema exists in nonexistent catalog")
			})
		})
		t.Run("list", func(t *testing.T) {
			t.Run("with nonexistent catalog", func(t *testing.T) {
				_, err := db.ListSchemas(ctx, sqlconnect.WithCatalog("nonexistent"))
				require.Error(t, err, "it should not be able to list schemas in nonexistent catalog")
			})
		})
	})
	t.Run("table admin", func(t *testing.T) {
		t.Run("list tables with nonexistent catalog", func(t *testing.T) {
			_, err := db.ListTables(ctx, sqlconnect.SchemaRef{Name: "test_schema"}, sqlconnect.WithCatalog("nonexistent"))
			require.Error(t, err, "it should not be able to list tables in nonexistent catalog")
		})
	})
}

func TestDatabricksDB(t *testing.T) {
	configJSON, ok := os.LookupEnv("DATABRICKS_TEST_ENVIRONMENT_CREDENTIALS")
	if !ok {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatal("DATABRICKS_TEST_ENVIRONMENT_CREDENTIALS environment variable not set")
		}
		t.Skip("skipping databricks integration test due to lack of a test environment")
	}

	t.Run("with catalog", func(t *testing.T) {
		configJSON, err := sjson.Set(configJSON, "retryAttempts", 4)
		require.NoError(t, err, "failed to set retryAttempts")
		configJSON, err = sjson.Set(configJSON, "minRetryWaitTime", time.Second)
		require.NoError(t, err, "failed to set minRetryWaitTime")
		configJSON, err = sjson.Set(configJSON, "maxRetryWaitTime", 30*time.Second)
		require.NoError(t, err, "failed to set maxRetryWaitTime")
		configJSON, err = sjson.Set(configJSON, "catalog", "sqlconnect")
		require.NoError(t, err, "failed to set catalog")
		_, err = sqlconnect.NewDB(databricks.DatabaseType, []byte(configJSON))
		require.NoError(t, err, "failed to create db")

		integrationtest.TestDatabaseScenarios(
			t,
			databricks.DatabaseType,
			[]byte(configJSON),
			strings.ToLower,
			integrationtest.Options{
				LegacySupport:                  true,
				SpecialCharactersInQuotedTable: "`-",
				SkipNonExistentCatalogTests:    true,
				ExtraTests:                     extraTests,
			},
		)
	})

	t.Run("with oauth", func(t *testing.T) {
		oauthConfigJSON, ok := os.LookupEnv("DATABRICKS_OAUTH_TEST_ENVIRONMENT_CREDENTIALS")
		if !ok {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatal("DATABRICKS_OAUTH_TEST_ENVIRONMENT_CREDENTIALS environment variable not set")
			}
			t.Skip("skipping databricks ouath integration test due to lack of a test environment")
		}
		configJSON, err := sjson.Set(oauthConfigJSON, "useOauth", true)
		require.NoError(t, err, "failed to set useOauth")
		_, err = sqlconnect.NewDB(databricks.DatabaseType, []byte(configJSON))
		require.NoError(t, err, "failed to create db")

		integrationtest.TestDatabaseScenarios(
			t,
			databricks.DatabaseType,
			[]byte(configJSON),
			strings.ToLower,
			integrationtest.Options{
				LegacySupport:                  true,
				SpecialCharactersInQuotedTable: "_A",
				ExtraTests:                     extraTests,
				SkipNonExistentCatalogTests:    true,
			},
		)
	})

	t.Run("default catalog", func(t *testing.T) {
		configJSON, err := sjson.Set(configJSON, "catalog", "hive_metastore")
		require.NoError(t, err, "failed to set catalog")
		_, err = sqlconnect.NewDB(databricks.DatabaseType, []byte(configJSON))
		require.NoError(t, err, "failed to create db")

		integrationtest.TestDatabaseScenarios(
			t,
			databricks.DatabaseType,
			[]byte(configJSON),
			strings.ToLower,
			integrationtest.Options{
				LegacySupport:                  true,
				SpecialCharactersInQuotedTable: "_A", // No special characters allowed
				ExtraTests:                     extraTests,
				SkipNonExistentCatalogTests:    true,
			},
		)

		integrationtest.TestSshTunnelScenarios(t, databricks.DatabaseType, []byte(configJSON))
	})

	t.Run("without catalog", func(t *testing.T) {
		configJSON, err := sjson.Set(configJSON, "catalog", "")
		require.NoError(t, err, "failed to set catalog")
		_, err = sqlconnect.NewDB(databricks.DatabaseType, []byte(configJSON))
		require.NoError(t, err, "failed to create db")

		integrationtest.TestDatabaseScenarios(
			t,
			databricks.DatabaseType,
			[]byte(configJSON),
			strings.ToLower,
			integrationtest.Options{
				LegacySupport:                  true,
				SpecialCharactersInQuotedTable: "_A",
				ExtraTests:                     extraTests,
				SkipNonExistentCatalogTests:    true,
			},
		)
	})
}
