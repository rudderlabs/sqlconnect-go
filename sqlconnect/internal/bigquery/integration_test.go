package bigquery_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/bigquery"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
	"github.com/stretchr/testify/require"
)

func TestBigqueryDB(t *testing.T) {
	configJSON, ok := os.LookupEnv("BIGQUERY_TEST_ENVIRONMENT_CREDENTIALS")

	if !ok {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatal("BIGQUERY_TEST_ENVIRONMENT_CREDENTIALS environment variable not set")
		}
		t.Skip("skipping bigquery integration test due to lack of a test environment")
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
		bigquery.DatabaseType,
		[]byte(configJSON),
		strings.ToLower,
		integrationtest.Options{
			LegacySupport:                  true,
			SpecialCharactersInQuotedTable: "-",
			ExtraTests:                     extraTests,
			SkipNonExistentCatalogTests:    true,
		},
	)
}
