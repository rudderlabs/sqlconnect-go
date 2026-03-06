package snowflake_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/snowflake"
)

func TestSnowflakeDB(t *testing.T) {
	configJSON, ok := os.LookupEnv("SNOWFLAKE_TEST_ENVIRONMENT_CREDENTIALS")
	if !ok {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatal("SNOWFLAKE_TEST_ENVIRONMENT_CREDENTIALS environment variable not set")
		}
		t.Skip("skipping snowflake integration test due to lack of a test environment")
	}

	extraTests := func(t *testing.T, db sqlconnect.DB) {
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
	integrationtest.TestDatabaseScenarios(
		t,
		snowflake.DatabaseType,
		[]byte(configJSON),
		strings.ToUpper,
		integrationtest.Options{
			LegacySupport:               true,
			ExtraTests:                  extraTests,
			SkipNonExistentCatalogTests: true,
		},
	)
}
