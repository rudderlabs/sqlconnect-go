package redshift_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
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

		integrationtest.TestDatabaseScenarios(
			t,
			redshift.DatabaseType,
			[]byte(configJSON),
			strings.ToLower,
			integrationtest.Options{
				LegacySupport: true,
				ExtraTests:    ExtraTests,
			},
		)

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
		integrationtest.TestDatabaseScenarios(
			t,
			redshift.DatabaseType,
			[]byte(configJSON),
			strings.ToLower,
			integrationtest.Options{
				LegacySupport: true,
				ExtraTests:    ExtraTests,
			},
		)
	})
}

func ExtraTests(t *testing.T, db sqlconnect.DB) {
	t.Run("list columns with non schema binding", func(t *testing.T) {
		ctx := context.Background()
		schema := sqlconnect.SchemaRef{Name: integrationtest.GenerateTestSchema(strings.ToLower)}
		err := db.CreateSchema(ctx, schema)
		require.NoErrorf(t, err, "it should be able to create schema")
		nonSchemaBindedView := sqlconnect.NewRelationRef(strings.ToLower("column_mappings_test_vw_ns"), sqlconnect.WithSchema(schema.Name))
		integrationtest.ExecuteStatements(t, db, schema.Name, "testdata/column-mapping-ns-view-test-seed.sql")

		expectedColsJSON, err := os.ReadFile("testdata/column-mapping-test-columns.json")
		require.NoErrorf(t, err, "it should be able to read the column mappings json file")
		var expectedColsMap map[string]string
		err = json.Unmarshal(expectedColsJSON, &expectedColsMap)
		require.NoErrorf(t, err, "it should be able to unmarshal the column mappings json file")
		expectedCols := lo.MapToSlice(expectedColsMap, func(k, v string) sqlconnect.ColumnRef {
			return sqlconnect.ColumnRef{Name: k, Type: v}
		})

		viewExists, err := db.TableExists(ctx, nonSchemaBindedView)
		require.NoError(t, err, "it should be able to check if a table exists")
		require.True(t, viewExists, "it should return true for a view that exists")

		selectViewStmt := sqlconnect.QueryDef{Table: nonSchemaBindedView, OrderBy: &sqlconnect.QueryOrder{Column: strings.ToLower("_order"), Order: "ASC"}}
		selectViewSQL := selectViewStmt.ToSQL(db)

		t.Run("list columns", func(t *testing.T) {
			actualCols, err := db.ListColumns(ctx, nonSchemaBindedView)
			require.NoError(t, err, "it should be able to list columns")
			actualCols = lo.Map(actualCols, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
				require.NotEmptyf(t, col.RawType, "it should return the raw type for column %q", col.Name)
				col.RawType = ""
				return col
			})
			require.ElementsMatch(t, actualCols, expectedCols, "it should return the correct columns")
		})

		t.Run("list columns for sql query", func(t *testing.T) {
			actualCols, err := db.ListColumnsForSqlQuery(ctx, selectViewSQL)
			actualCols = lo.Map(actualCols, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
				require.NotEmptyf(t, col.RawType, "it should return the raw type for column %q", col.Name)
				col.RawType = ""
				return col
			})
			require.NoError(t, err, "it should be able to list columns")
			require.ElementsMatch(t, actualCols, expectedCols, "it should return the correct columns")
		})
	})
}
