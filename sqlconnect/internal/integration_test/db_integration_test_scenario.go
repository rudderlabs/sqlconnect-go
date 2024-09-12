package integrationtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/op"
	sqlconnectutil "github.com/rudderlabs/sqlconnect-go/sqlconnect/util"
)

type Options struct {
	// LegacySupport enables the use of legacy column mappings
	LegacySupport bool

	SpecialCharactersInQuotedTable string // special characters to test in quoted table identifiers (default: <space>,",',``)

	ExtraTests func(t *testing.T, db sqlconnect.DB)
}

func TestDatabaseScenarios(t *testing.T, warehouse string, configJSON json.RawMessage, formatfn func(string) string, opts Options) {
	schema := sqlconnect.SchemaRef{Name: GenerateTestSchema(formatfn)}
	db, err := sqlconnect.NewDB(warehouse, configJSON)
	require.NoError(t, err, "it should be able to create a new DB")
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("using invalid configuration", func(t *testing.T) {
		_, err := sqlconnect.NewDB(warehouse, []byte("invalid"))
		require.Error(t, err, "it should return error for invalid configuration")
	})

	t.Run("ping", func(t *testing.T) {
		t.Run("with context cancelled", func(t *testing.T) {
			err := db.PingContext(cancelledCtx)
			require.Error(t, err, "it should not be able to ping the database with a cancelled context")
		})

		t.Run("normal operation", func(t *testing.T) {
			err := db.Ping()
			require.NoError(t, err, "it should be able to ping the database")

			err = db.PingContext(ctx)
			require.NoError(t, err, "it should be able to ping the database")
		})
	})

	var currentCatalog string
	t.Run("catalog admin", func(t *testing.T) {
		t.Run("current catalog", func(t *testing.T) {
			t.Run("with context cancelled", func(t *testing.T) {
				_, err := db.CurrentCatalog(cancelledCtx)
				require.Error(t, err, "it should not be able to get the current catalog with a cancelled context")
			})

			currentCatalog, err = db.CurrentCatalog(ctx)
			if errors.Is(err, sqlconnect.ErrNotSupported) {
				t.Skipf("skipping test for warehouse %s: %v", warehouse, err)
			}
			require.NoError(t, err, "it should be able to get the current catalog")
			require.NotEmpty(t, currentCatalog, "it should return a non-empty current catalog")
		})
	})

	t.Run("schema admin", func(t *testing.T) {
		t.Run("schema doesn't exist", func(t *testing.T) {
			exists, err := db.SchemaExists(ctx, schema)
			require.NoError(t, err, "it should be able to check if a schema exists")
			require.False(t, exists, "it should return false for a schema that doesn't exist")
		})

		t.Run("create", func(t *testing.T) {
			t.Run("with context cancelled", func(t *testing.T) {
				err := db.CreateSchema(cancelledCtx, schema)
				require.Error(t, err, "it should not be able to create a schema with a cancelled context")
			})

			t.Run("normal operation", func(t *testing.T) {
				err := db.CreateSchema(ctx, schema)
				require.NoError(t, err, "it should be able to create a schema")
			})

			t.Run("idempotence", func(t *testing.T) {
				err := db.CreateSchema(ctx, schema)
				require.NoError(t, err, "it shouldn't fail if the schema already exists")
			})
		})
		t.Run("exists", func(t *testing.T) {
			exists, err := db.SchemaExists(ctx, schema)
			require.NoError(t, err, "it should be able to check if a schema exists")
			require.True(t, exists, "it should return true for a schema that exists")

			t.Run("with context cancelled", func(t *testing.T) {
				_, err := db.SchemaExists(cancelledCtx, schema)
				require.Error(t, err, "it should not be able to check if a schema exists with a cancelled context")
			})
		})

		t.Run("list", func(t *testing.T) {
			schemas, err := db.ListSchemas(ctx)
			require.NoError(t, err, "it should be able to list schemas")
			require.Contains(t, schemas, schema, "it should contain the created schema")
			t.Run("with context cancelled", func(t *testing.T) {
				_, err := db.ListSchemas(cancelledCtx)
				require.Error(t, err, "it should not be able to list schemas with a cancelled context")
			})
		})

		t.Run("drop", func(t *testing.T) {
			t.Run("with context cancelled", func(t *testing.T) {
				err := db.DropSchema(cancelledCtx, schema)
				require.Error(t, err, "it should not be able to drop a schema with a cancelled context")
			})

			t.Run("normal operation", func(t *testing.T) {
				otherSchema := sqlconnect.SchemaRef{Name: GenerateTestSchema(formatfn)}
				err := db.CreateSchema(ctx, otherSchema)
				require.NoError(t, err, "it should be able to create a schema")
				err = db.DropSchema(ctx, otherSchema)
				require.NoError(t, err, "it should be able to drop a schema")
			})

			t.Run("invalid schema name", func(t *testing.T) {
				err := db.DropSchema(ctx, sqlconnect.SchemaRef{Name: "nonexistent"})
				require.Error(t, err, "it shouldn't be able to drop a non-existent schema")
			})
		})
	})

	t.Run("goqu dialect", func(t *testing.T) {
		table := sqlconnect.NewRelationRef(formatfn("goqu_test"), sqlconnect.WithSchema(schema.Name))
		ExecuteStatements(t, db, schema.Name, "testdata/goqu-test-seed.sql")

		const (
			stringCol = "_string"
			stringVal = "string"

			intCol = "_int"
			intVal = 1

			floatCol = "_float"
			floatVal = 1.1

			boolCol = "_boolean"
			boolVal = true

			timeCol = "_timestamp"
			timeVal = "2021-01-01T00:00:00Z"
		)
		timestampVal, err := time.Parse(time.RFC3339, timeVal)
		require.NoError(t, err, "it should be able to parse the timestamp value")

		validateCondition := func(t *testing.T, condition string, count int) {
			rows, err := db.GetRowCountForQuery(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", table, condition))
			require.NoErrorf(t, err, "it should be able to get row count for query with condition %q", condition)
			require.Equal(t, count, rows, "it should return %d rows for query with condition %q", count, condition)
		}

		getQueryCondition := func(t *testing.T, col, op string, val ...any) string {
			sql, err := db.QueryCondition(col, op, val...)
			require.NoError(t, err, "it should be able to generate a query condition")
			return sql
		}

		getTimestampAddExpression := func(t *testing.T, timeValue any, interval int, unit string) any {
			expr, err := db.Expressions().TimestampAdd(timeValue, interval, unit)
			require.NoError(t, err, "it should be able to generate a time add expression")
			return expr
		}

		getDateAddExpression := func(t *testing.T, dateValue any, interval int, unit string) any {
			expr, err := db.Expressions().DateAdd(dateValue, interval, unit)
			require.NoError(t, err, "it should be able to generate a date add expression")
			return expr
		}

		t.Run("isset", func(t *testing.T) {
			op := string(op.IsSet)
			rowCount := 1
			validateCondition(t, getQueryCondition(t, stringCol, op), rowCount)
			validateCondition(t, getQueryCondition(t, intCol, op), rowCount)
			validateCondition(t, getQueryCondition(t, floatCol, op), rowCount)
			validateCondition(t, getQueryCondition(t, boolCol, op), rowCount)
			validateCondition(t, getQueryCondition(t, timeCol, op), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op, "invalid")
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("notset", func(t *testing.T) {
			op := string(op.NotSet)
			rowCount := 0
			validateCondition(t, getQueryCondition(t, stringCol, op), rowCount)
			validateCondition(t, getQueryCondition(t, intCol, op), rowCount)
			validateCondition(t, getQueryCondition(t, floatCol, op), rowCount)
			validateCondition(t, getQueryCondition(t, boolCol, op), rowCount)
			validateCondition(t, getQueryCondition(t, timeCol, op), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op, "invalid")
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("eq", func(t *testing.T) {
			op := string(op.Eq)
			rowCount := 1
			validateCondition(t, getQueryCondition(t, stringCol, op, stringVal), rowCount)
			validateCondition(t, getQueryCondition(t, intCol, op, intVal), rowCount)
			validateCondition(t, getQueryCondition(t, floatCol, op, floatVal), rowCount)
			validateCondition(t, getQueryCondition(t, boolCol, op, boolVal), rowCount)
			validateCondition(t, getQueryCondition(t, timeCol, op, timestampVal), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op, "one", "two")
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("neq", func(t *testing.T) {
			op := string(op.Neq)
			rowCount := 0
			validateCondition(t, getQueryCondition(t, stringCol, op, stringVal), rowCount)
			validateCondition(t, getQueryCondition(t, intCol, op, intVal), rowCount)
			validateCondition(t, getQueryCondition(t, floatCol, op, floatVal), rowCount)
			validateCondition(t, getQueryCondition(t, boolCol, op, boolVal), rowCount)
			validateCondition(t, getQueryCondition(t, timeCol, op, timestampVal), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op, "one", "two")
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("in", func(t *testing.T) {
			op := string(op.In)
			rowCount := 1
			validateCondition(t, getQueryCondition(t, stringCol, op, stringVal), rowCount)
			validateCondition(t, getQueryCondition(t, intCol, op, intVal), rowCount)
			validateCondition(t, getQueryCondition(t, floatCol, op, floatVal), rowCount)
			validateCondition(t, getQueryCondition(t, boolCol, op, boolVal), rowCount)
			// in for timestamps is not supported for databricks
			// validateCondition(t, getQueryCondition(t, timeCol, op, timestampVal), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op)
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("notin", func(t *testing.T) {
			op := string(op.NotIn)
			rowCount := 0
			validateCondition(t, getQueryCondition(t, stringCol, op, stringVal), rowCount)
			validateCondition(t, getQueryCondition(t, intCol, op, intVal), rowCount)
			validateCondition(t, getQueryCondition(t, floatCol, op, floatVal), rowCount)
			validateCondition(t, getQueryCondition(t, boolCol, op, boolVal), rowCount)
			// in for timestamps is not supported for databricks
			// validateCondition(t, getQueryCondition(t, timeCol, op, timestampVal), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op)
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("like", func(t *testing.T) {
			op := string(op.Like)
			rowCount := 1
			validateCondition(t, getQueryCondition(t, stringCol, op, stringVal), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op)
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("notlike", func(t *testing.T) {
			op := string(op.NotLike)
			rowCount := 0
			validateCondition(t, getQueryCondition(t, stringCol, op, stringVal), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op)
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("gt", func(t *testing.T) {
			op := string(op.Gt)
			rowCount := 1
			validateCondition(t, getQueryCondition(t, intCol, op, intVal-1), rowCount)
			validateCondition(t, getQueryCondition(t, floatCol, op, floatVal-1.0), rowCount)
			validateCondition(t, getQueryCondition(t, timeCol, op, timestampVal.Add(-1*time.Hour)), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op)
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("gte", func(t *testing.T) {
			op := string(op.Gte)
			rowCount := 1
			validateCondition(t, getQueryCondition(t, intCol, op, intVal), rowCount)
			validateCondition(t, getQueryCondition(t, floatCol, op, floatVal), rowCount)
			validateCondition(t, getQueryCondition(t, timeCol, op, timestampVal), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op)
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("lt", func(t *testing.T) {
			op := string(op.Lt)
			rowCount := 1
			validateCondition(t, getQueryCondition(t, intCol, op, intVal+1), rowCount)
			validateCondition(t, getQueryCondition(t, floatCol, op, floatVal+1.0), rowCount)
			validateCondition(t, getQueryCondition(t, timeCol, op, timestampVal.Add(time.Hour)), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op)
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("lte", func(t *testing.T) {
			op := string(op.Lte)
			rowCount := 1
			validateCondition(t, getQueryCondition(t, intCol, op, intVal), rowCount)
			validateCondition(t, getQueryCondition(t, floatCol, op, floatVal), rowCount)
			validateCondition(t, getQueryCondition(t, timeCol, op, timestampVal), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op)
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("between", func(t *testing.T) {
			op := string(op.Between)
			rowCount := 1
			validateCondition(t, getQueryCondition(t, intCol, op, intVal-1, intVal+1), rowCount)
			validateCondition(t, getQueryCondition(t, floatCol, op, floatVal-1.0, floatVal+1.0), rowCount)
			validateCondition(t, getQueryCondition(t, timeCol, op, timestampVal.Add(-1*time.Hour), timestampVal.Add(time.Hour)), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op)
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("notbetween", func(t *testing.T) {
			op := string(op.NotBetween)
			rowCount := 0
			validateCondition(t, getQueryCondition(t, intCol, op, intVal-1, intVal+1), rowCount)
			validateCondition(t, getQueryCondition(t, floatCol, op, floatVal-1.0, floatVal+1.0), rowCount)
			validateCondition(t, getQueryCondition(t, timeCol, op, timestampVal.Add(-1*time.Hour), timestampVal.Add(time.Hour)), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition(stringCol, op)
				require.Error(t, err, "it should return an error for invalid arguments")
			})
		})

		t.Run("invalid operator", func(t *testing.T) {
			_, err = db.QueryCondition("column", "someop")
			require.Error(t, err, "it should return an error for an invalid operator")
			require.ErrorContains(t, err, "unsupported operator: someop", "it should return an error for an invalid operator")
		})

		t.Run("nbfinterval operator", func(t *testing.T) {
			op := string(op.NbfInterval)
			rowCount := 0
			validateCondition(t, getQueryCondition(t, "DATE("+timeCol+")", op, 1, "day"), rowCount)

			t.Run("with invalid arguments", func(t *testing.T) {
				_, err := db.QueryCondition("col", op)
				require.Error(t, err, "it should return an error for no arguments")

				_, err = db.QueryCondition("col", op, "1", "day")
				require.Error(t, err, "it should return an error for invalid 1st argument")

				_, err = db.QueryCondition("col", op, 1, 2)
				require.Error(t, err, "it should return an error for invalid 2nd argument")

				_, err = db.QueryCondition("col", op, 1, "day", 3)
				require.Error(t, err, "it should return an error for invalid number of arguments")
			})
		})

		t.Run("time add", func(t *testing.T) {
			op := string(op.Lt)
			rowCount := 1

			validateCondition(t, getQueryCondition(t, timeCol, op, getTimestampAddExpression(t, timestampVal, 1, "hour")), rowCount)
			validateCondition(t, getQueryCondition(t, timeCol, op, getTimestampAddExpression(t, "CURRENT_TIMESTAMP", -1, "day")), rowCount)
		})

		t.Run("date add", func(t *testing.T) {
			op := string(op.Lt)
			rowCount := 1

			validateCondition(t, getQueryCondition(t, "DATE("+timeCol+")", op, getDateAddExpression(t, timestampVal, 1, "day")), rowCount)
			validateCondition(t, getQueryCondition(t, "DATE("+timeCol+")", op, getDateAddExpression(t, "CURRENT_TIMESTAMP", -1, "day")), rowCount)
		})
	})

	t.Run("dialect", func(t *testing.T) {
		t.Run("with unquoted table", func(t *testing.T) {
			identifier := db.QuoteIdentifier(schema.Name) + "." + "UnQuoted_TablE"
			_, err := db.Exec("CREATE TABLE " + identifier + " (c1 int)")
			require.NoError(t, err, "it should be able to create an unquoted table")

			table, err := db.ParseRelationRef(identifier)
			require.NoError(t, err, "it should be able to parse an unquoted table")

			alltables, err := db.ListTables(ctx, schema)
			require.NoError(t, err, "it should be able to list tables")

			exists, err := db.TableExists(ctx, table)
			require.NoErrorf(t, err, "it should be able to check if a table exists: %s allTables: %+v", table, alltables)
			require.Truef(t, exists, "it should return true for a table that exists: %s allTables: %+v", table, alltables)
		})

		t.Run("with quoted table", func(t *testing.T) {
			identifier := db.QuoteIdentifier(schema.Name) + "." + db.QuoteIdentifier("Quoted_TablE")
			_, err := db.Exec("CREATE TABLE " + identifier + " (c1 int)")
			require.NoErrorf(t, err, "it should be able to create a quoted table: %s", identifier)

			table, err := db.ParseRelationRef(identifier)
			require.NoError(t, err, "it should be able to parse a quoted table")

			alltables, err := db.ListTables(ctx, schema)
			require.NoError(t, err, "it should be able to list tables")

			exists, err := db.TableExists(ctx, table)
			require.NoErrorf(t, err, "it should be able to check if a table exists: %s allTables: %+v", table, alltables)
			require.Truef(t, exists, "it should return true for a table that exists: %s allTables: %+v", table, alltables)
		})

		t.Run("with quoted table and special characters", func(t *testing.T) {
			specialCharacters := " \"`'"
			if len(opts.SpecialCharactersInQuotedTable) > 0 {
				specialCharacters = opts.SpecialCharactersInQuotedTable
			}

			identifier := db.QuoteIdentifier(schema.Name) + "." + db.QuoteIdentifier("Quoted_TablE"+specialCharacters)
			_, err := db.Exec("CREATE TABLE " + identifier + " (c1 int)")
			require.NoErrorf(t, err, "it should be able to create a quoted table: %s", identifier)

			table, err := db.ParseRelationRef(identifier)
			require.NoError(t, err, "it should be able to parse a quoted table")

			alltables, err := db.ListTables(ctx, schema)
			require.NoError(t, err, "it should be able to list tables")

			exists, err := db.TableExists(ctx, table)
			require.NoErrorf(t, err, "it should be able to check if a table exists: %s allTables: %+v", table, alltables)
			require.Truef(t, exists, "it should return true for a table that exists: %s allTables: %+v", table, alltables)
		})
	})

	t.Run("table admin", func(t *testing.T) {
		table := sqlconnect.NewRelationRef(formatfn("test_table"), sqlconnect.WithSchema(schema.Name))
		view := sqlconnect.NewRelationRef(formatfn("test_view"), sqlconnect.WithSchema(schema.Name))

		t.Run("table doesn't exist", func(t *testing.T) {
			t.Run("with context cancelled", func(t *testing.T) {
				_, err := db.TableExists(cancelledCtx, table)
				require.Error(t, err, "it should not be able to check if a table exists with a cancelled context")
			})

			exists, err := db.TableExists(ctx, table)
			require.NoError(t, err, "it should be able to check if a table exists")
			require.False(t, exists, "it should return false for a table that doesn't exist")
		})

		t.Run("create test table", func(t *testing.T) {
			t.Run("with context cancelled", func(t *testing.T) {
				err := db.CreateTestTable(cancelledCtx, table)
				require.Error(t, err, "it should not be able to create a test table with a cancelled context")
			})

			err := db.CreateTestTable(ctx, table)
			require.NoError(t, err, "it should be able to create a test table")
			exists, err := db.TableExists(ctx, table)
			require.NoError(t, err, "it should be able to check if a table exists")
			require.True(t, exists, "it should return true for a table that was just created")
		})

		t.Run("create view", func(t *testing.T) {
			_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM %s", db.QuoteTable(view), db.QuoteTable(table)))
			require.NoError(t, err, "it should be able to create a view")
		})

		t.Run("list tables", func(t *testing.T) {
			t.Run("with context cancelled", func(t *testing.T) {
				_, err := db.ListTables(cancelledCtx, schema)
				require.Error(t, err, "it should not be able to list tables with a cancelled context")
			})

			tables, err := db.ListTables(ctx, schema)
			require.NoError(t, err, "it should be able to list tables")
			require.Contains(t, tables, table, "it should contain the created table")
		})

		t.Run("list tables with views", func(t *testing.T) {
			tables, err := db.ListTables(ctx, schema)
			require.NoError(t, err, "it should be able to list tables")
			require.Contains(t, tables, view, "it should contain the created view")
			require.Contains(t, tables, table, "it should contain the table as well")
		})

		t.Run("list tables with prefix", func(t *testing.T) {
			t.Run("with context cancelled", func(t *testing.T) {
				_, err := db.ListTablesWithPrefix(cancelledCtx, schema, formatfn("test"))
				require.Error(t, err, "it should not be able to list tables with a prefix with a cancelled context")
			})

			tables, err := db.ListTablesWithPrefix(ctx, schema, formatfn("test"))
			require.NoError(t, err, "it should be able to list tables with a prefix")
			require.Contains(t, tables, table, "it should contain the created table")
		})

		t.Run("list columns", func(t *testing.T) {
			t.Run("with nonexistent relation", func(t *testing.T) {
				nonExistentRelation := sqlconnect.NewRelationRef(formatfn("foobar"), sqlconnect.WithSchema(schema.Name))
				_, err := db.ListColumns(ctx, nonExistentRelation)
				require.Error(t, err, "it should throw an error when columns are listed for a nonexistent relation")
			})

			t.Run("with context cancelled", func(t *testing.T) {
				_, err := db.ListColumns(cancelledCtx, table)
				require.Error(t, err, "it should not be able to list columns with a cancelled context")
			})

			t.Run("without catalog", func(t *testing.T) {
				columns, err := db.ListColumns(ctx, table)
				columns = lo.Map(columns, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
					require.NotEmptyf(t, col.RawType, "it should return the raw type for column %q", col.Name)
					col.RawType = ""
					return col
				})
				require.NoError(t, err, "it should be able to list columns")
				require.Len(t, columns, 2, "it should return the correct number of columns")
				require.ElementsMatch(t, columns, []sqlconnect.ColumnRef{
					{Name: formatfn("c1"), Type: "int"},
					{Name: formatfn("c2"), Type: "string"},
				}, "it should return the correct columns")
			})

			t.Run("with catalog", func(t *testing.T) {
				tableWithCatalog := table
				tableWithCatalog.Catalog = currentCatalog
				columns, err := db.ListColumns(ctx, tableWithCatalog)
				require.NoErrorf(t, err, "it should be able to list columns for %s", tableWithCatalog)
				columns = lo.Map(columns, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
					require.NotEmptyf(t, col.RawType, "it should return the raw type for column %q", col.Name)
					col.RawType = ""
					return col
				})

				require.Len(t, columns, 2, "it should return the correct number of columns")
				require.ElementsMatch(t, columns, []sqlconnect.ColumnRef{
					{Name: formatfn("c1"), Type: "int"},
					{Name: formatfn("c2"), Type: "string"},
				}, "it should return the correct columns")
			})

			t.Run("with invalid catalog", func(t *testing.T) {
				tableWithInvalidCatalog := table
				tableWithInvalidCatalog.Catalog = "invalid"
				cols, _ := db.ListColumns(ctx, tableWithInvalidCatalog)
				require.Empty(t, cols, "it should return an empty list of columns for an invalid catalog")
			})

			t.Run("list columns for view", func(t *testing.T) {
				columns, err := db.ListColumns(ctx, view)
				columns = lo.Map(columns, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
					require.NotEmptyf(t, col.RawType, "it should return the raw type for column %q", col.Name)
					col.RawType = ""
					return col
				})
				require.NoError(t, err, "it should be able to list columns for a view")
				require.Len(t, columns, 2, "it should return the correct number of columns")
				require.ElementsMatch(t, columns, []sqlconnect.ColumnRef{
					{Name: formatfn("c1"), Type: "int"},
					{Name: formatfn("c2"), Type: "string"},
				}, "it should return the correct columns")
			})
		})

		t.Run("list columns for sql query", func(t *testing.T) {
			q := sqlconnect.QueryDef{
				Table:   table,
				Columns: []string{formatfn("c1")},
			}
			stmt := q.ToSQL(db)

			t.Run("with context cancelled", func(t *testing.T) {
				_, err := db.ListColumnsForSqlQuery(cancelledCtx, stmt)
				require.Error(t, err, "it should not be able to list columns for a sql query with a cancelled context")
			})

			columns, err := db.ListColumnsForSqlQuery(ctx, stmt)
			columns = lo.Map(columns, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
				require.NotEmptyf(t, col.RawType, "it should return the raw type for column %q", col.Name)
				col.RawType = ""
				return col
			})
			require.NoError(t, err, "it should be able to list columns for a sql query")
			require.Len(t, columns, 1, "it should return the correct number of columns")
			require.ElementsMatch(t, columns, []sqlconnect.ColumnRef{
				{Name: formatfn("c1"), Type: "int"},
			}, "it should return the correct columns")
		})

		t.Run("count table rows", func(t *testing.T) {
			t.Run("with context cancelled", func(t *testing.T) {
				_, err := db.CountTableRows(cancelledCtx, table)
				require.Error(t, err, "it should not be able to count table rows with a cancelled context")
			})

			count, err := db.CountTableRows(ctx, table)
			require.NoError(t, err, "it should be able to count table rows")
			require.Equal(t, 0, count, "it should return 0 for a table with no rows")

			// add a row
			_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (c1, c2) VALUES (1, '1')", db.QuoteTable(table)))
			require.NoError(t, err, "it should be able to insert a row")

			count, err = db.CountTableRows(ctx, table)
			require.NoError(t, err, "it should be able to count table rows")
			require.Equal(t, 1, count, "it should return 1 for a table with one row")
		})

		t.Run("truncate table", func(t *testing.T) {
			t.Run("with context cancelled", func(t *testing.T) {
				err := db.TruncateTable(cancelledCtx, table)
				require.Error(t, err, "it should not be able to truncate a table with a cancelled context")
			})

			err := db.TruncateTable(ctx, table)
			require.NoError(t, err, "it should be able to truncate a table")
			count, err := db.CountTableRows(ctx, table)
			require.NoError(t, err, "it should be able to count table rows")
			require.Equal(t, 0, count, "it should return 0 for a table with no rows")
		})

		t.Run("rename table", func(t *testing.T) {
			table := sqlconnect.NewRelationRef(formatfn("test_table_torename"), sqlconnect.WithSchema(schema.Name))
			err := db.CreateTestTable(ctx, table)
			require.NoError(t, err, "it should be able to create a test table")
			newTable := sqlconnect.NewRelationRef(formatfn("test_table_renamed"), sqlconnect.WithSchema(schema.Name))

			t.Run("with context cancelled", func(t *testing.T) {
				err := db.RenameTable(cancelledCtx, table, newTable)
				require.Error(t, err, "it should not be able to rename a table with a cancelled context")
			})

			t.Run("using different schemas", func(t *testing.T) {
				newTableWithDifferentSchema := newTable
				newTableWithDifferentSchema.Schema = newTableWithDifferentSchema.Schema + "_other"
				err := db.RenameTable(ctx, table, newTableWithDifferentSchema)
				require.Error(t, err, "it should not be able to rename a table to a different schema")
			})

			t.Run("normal operation", func(t *testing.T) {
				err := db.RenameTable(ctx, table, newTable)
				require.NoError(t, err, "it should be able to rename a table")

				exists, err := db.TableExists(ctx, newTable)
				require.NoError(t, err, "it should be able to check if a table exists")
				require.True(t, exists, "it should return true for a table that was just renamed")

				exists, err = db.TableExists(ctx, table)
				require.NoError(t, err, "it should be able to check if the old table exists")
				require.False(t, exists, "it should return false for the old table which was just renamed")
			})
		})

		t.Run("move table", func(t *testing.T) {
			table := sqlconnect.NewRelationRef(formatfn("test_table_torename"), sqlconnect.WithSchema(schema.Name))
			err := db.CreateTestTable(ctx, table)
			require.NoError(t, err, "it should be able to create a test table")
			cols, err := db.ListColumns(ctx, table)
			require.NoError(t, err, "it should be able to list columns")

			newTable := sqlconnect.NewRelationRef(formatfn("test_table_moved"), sqlconnect.WithSchema(schema.Name))

			t.Run("with context cancelled", func(t *testing.T) {
				err := db.MoveTable(cancelledCtx, table, newTable)
				require.Error(t, err, "it should not be able to move a table with a cancelled context")
			})

			t.Run("using different schemas", func(t *testing.T) {
				newTableWithDifferentSchema := newTable
				newTableWithDifferentSchema.Schema = newTableWithDifferentSchema.Schema + "_other"
				err := db.MoveTable(ctx, table, newTableWithDifferentSchema)
				require.Error(t, err, "it should not be able to move a table to a different schema")
			})

			t.Run("normal operation", func(t *testing.T) {
				err := db.MoveTable(ctx, table, newTable)
				require.NoError(t, err, "it should be able to rename a table")

				exists, err := db.TableExists(ctx, newTable)
				require.NoError(t, err, "it should be able to check if a table exists")
				require.True(t, exists, "it should return true for a table that was just moved")

				newCols, err := db.ListColumns(ctx, newTable)
				require.NoError(t, err, "it should be able to list columns")
				require.ElementsMatch(t, newCols, cols, "it should return the same columns for the new table")

				exists, err = db.TableExists(ctx, table)
				require.NoError(t, err, "it should be able to check if the old table exists")
				require.False(t, exists, "it should return false for the old table which was just moved")
			})
		})

		t.Run("drop table", func(t *testing.T) {
			table := sqlconnect.NewRelationRef(formatfn("test_table_todrop"), sqlconnect.WithSchema(schema.Name))
			err := db.CreateTestTable(ctx, table)
			require.NoError(t, err, "it should be able to create a test table")

			t.Run("with context cancelled", func(t *testing.T) {
				err := db.DropTable(cancelledCtx, table)
				require.Error(t, err, "it should not be able to drop a table with a cancelled context")
			})

			err = db.DropTable(ctx, table)
			require.NoError(t, err, "it should be able to drop a table")
			exists, err := db.TableExists(ctx, table)
			require.NoError(t, err, "it should be able to check if a table exists")
			require.False(t, exists, "it should return false for a table that was just dropped")
		})

		table2 := sqlconnect.NewRelationRef(formatfn("test_table_2"), sqlconnect.WithSchema(schema.Name))
		t.Run("create table from query", func(t *testing.T) {
			table := sqlconnect.NewRelationRef(formatfn("test_table_from_query"), sqlconnect.WithSchema(schema.Name))
			t.Run("with context cancelled", func(t *testing.T) {
				err := db.CreateTableFromQuery(cancelledCtx, table2, "SELECT 1")
				require.Error(t, err, "it should not be able to create a table from a query with a cancelled context")
			})

			err := db.CreateTableFromQuery(ctx, table, "SELECT 1 AS numcol")
			require.NoError(t, err, "it should be able to create a table from a query")
			exists, err := db.TableExists(ctx, table)
			require.NoError(t, err, "it should be able to check if a table exists")
			require.True(t, exists, "it should return true for a table that was just created from a query")
		})

		t.Run("get row count for query", func(t *testing.T) {
			t.Run("with context cancelled", func(t *testing.T) {
				_, err := db.GetRowCountForQuery(cancelledCtx, "SELECT 1")
				require.Error(t, err, "it should not be able to get row count for a query with a cancelled context")
			})

			count, err := db.GetRowCountForQuery(ctx, "SELECT 2")
			require.NoError(t, err, "it should be able to get row count for a query")
			require.Equal(t, 2, count, "it should return the correct row count for a query")
		})
	})

	t.Run("column mapping", func(t *testing.T) {
		table := sqlconnect.NewRelationRef(formatfn("column_mappings_test"), sqlconnect.WithSchema(schema.Name))
		ExecuteStatements(t, db, schema.Name, "testdata/column-mapping-test-seed.sql")

		expectedColsJSON, err := os.ReadFile("testdata/column-mapping-test-columns.json")
		require.NoErrorf(t, err, "it should be able to read the column mappings json file")
		var expectedColsMap map[string]string
		err = json.Unmarshal(expectedColsJSON, &expectedColsMap)
		require.NoErrorf(t, err, "it should be able to unmarshal the column mappings json file")
		expectedCols := lo.MapToSlice(expectedColsMap, func(k, v string) sqlconnect.ColumnRef {
			return sqlconnect.ColumnRef{Name: k, Type: v}
		})

		exists, err := db.TableExists(ctx, table)
		require.NoError(t, err, "it should be able to check if a table exists")
		require.True(t, exists, "it should return true for a table that exists")

		selectStmt := sqlconnect.QueryDef{Table: table, OrderBy: &sqlconnect.QueryOrder{Column: formatfn("_order"), Order: "ASC"}}
		selectSQL := selectStmt.ToSQL(db)

		t.Run("list columns", func(t *testing.T) {
			actualCols, err := db.ListColumns(ctx, table)
			require.NoError(t, err, "it should be able to list columns")
			actualCols = lo.Map(actualCols, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
				require.NotEmptyf(t, col.RawType, "it should return the raw type for column %q", col.Name)
				col.RawType = ""
				return col
			})
			require.ElementsMatch(t, actualCols, expectedCols, "it should return the correct columns")
		})

		t.Run("list columns for sql query", func(t *testing.T) {
			actualCols, err := db.ListColumnsForSqlQuery(ctx, selectSQL)
			actualCols = lo.Map(actualCols, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
				require.NotEmptyf(t, col.RawType, "it should return the raw type for column %q", col.Name)
				col.RawType = ""
				return col
			})
			require.NoError(t, err, "it should be able to list columns")
			require.ElementsMatch(t, actualCols, expectedCols, "it should return the correct columns")
		})

		t.Run("json mapper", func(t *testing.T) {
			expectedRowsJSON, err := os.ReadFile("testdata/column-mapping-test-rows.json")
			require.NoErrorf(t, err, "it should be able to read the rows json file")

			ch, leave := sqlconnect.QueryJSONAsync(ctx, db, selectSQL)
			defer leave()
			var rows []any
			for row := range ch {
				require.NoError(t, row.Err, "it should be able to scan a row")
				var o any
				err := json.Unmarshal(row.Value, &o)
				require.NoError(t, err, "it should be able to unmarshal the row")
				rows = append(rows, o)
			}
			actualRowsJSON, err := json.Marshal(rows)
			require.NoError(t, err, "it should be able to marshal the rows")

			require.JSONEq(t, string(expectedRowsJSON), string(actualRowsJSON), "it should return the correct rows: "+string(actualRowsJSON))

			// verify that the json types are in parity with the column types
			cols, err := db.ListColumnsForSqlQuery(ctx, selectSQL)
			require.NoError(t, err, "it should be able to list columns")
			var actualRows []map[string]any
			require.NoError(t, json.Unmarshal(actualRowsJSON, &actualRows))
			require.Greater(t, len(actualRows), 0, "it should return at least one row")
			actualRow := actualRows[0]

			for _, col := range cols {
				switch col.Type {
				case "int":
					f, ok := actualRow[col.Name].(float64)
					require.Truef(t, ok, "column of type int should be parsed as a float64 %q: %v", col.Name, actualRow[col.Name])
					require.Equalf(t, float64(int(f)), f, "column of type int should be an integer %q: %v", col.Name, actualRow[col.Name])
				case "float":
					_, ok := actualRow[col.Name].(float64)
					require.Truef(t, ok, "column of type float should be parsed as a float64 %q: %v", col.Name, actualRow[col.Name])
				case "string":
					_, ok := actualRow[col.Name].(string)
					require.Truef(t, ok, "column of type string should be parsed as a string %q: %v", col.Name, actualRow[col.Name])
				case "boolean":
					_, ok := actualRow[col.Name].(bool)
					require.Truef(t, ok, "column of type boolean should be parsed as a bool %q: %v", col.Name, actualRow[col.Name])
				case "datetime":
					datetime, ok := actualRow[col.Name].(string)
					require.Truef(t, ok, "column of type datetime should be parsed as a datetime %q: %v", col.Name, actualRow[col.Name])
					_, err := time.Parse(time.RFC3339, datetime)
					require.NoErrorf(t, err, "column of type datetime should be a RFC3339 string %q: %v", col.Name, actualRow[col.Name])
				case "array":
					require.Truef(t, reflect.TypeOf(actualRow[col.Name]).Kind() == reflect.Slice, "column of type array should be a slice %q: %v", col.Name, actualRow[col.Name])

				case "json":
					// this can be anything
				default:
					t.Errorf("unexpected column type %s for column  %q: %v", col.Type, col.Name, actualRow[col.Name])
				}
			}
		})

		t.Run("legacy column and json mapper", func(t *testing.T) {
			if !opts.LegacySupport {
				t.Skip("legacy column and json mapper test skipped for warehouse " + warehouse)
			}
			altConfigJSON, err := sjson.SetBytes(configJSON, "useLegacyMappings", true)
			require.NoError(t, err, "it should be able to set useLegacyMappings")
			legacyDB, err := sqlconnect.NewDB(warehouse, altConfigJSON)
			require.NoError(t, err, "it should be able to create a new DB")
			defer func() { _ = legacyDB.Close() }()

			t.Run("list columns", func(t *testing.T) {
				expectedColsJSON, err := os.ReadFile("testdata/legacy-column-mapping-test-columns-table.json")
				require.NoErrorf(t, err, "it should be able to read the legacy column mappings json file")
				var expectedColsMap map[string]string
				err = json.Unmarshal(expectedColsJSON, &expectedColsMap)
				require.NoErrorf(t, err, "it should be able to unmarshal the legacy column mappings json file")
				expectedCols := lo.MapToSlice(expectedColsMap, func(k, v string) sqlconnect.ColumnRef {
					return sqlconnect.ColumnRef{Name: k, Type: v}
				})
				t.Run("without catalog", func(t *testing.T) {
					actualCols, err := legacyDB.ListColumns(ctx, table)
					require.NoError(t, err, "it should be able to list columns")
					actualCols = lo.Map(actualCols, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
						require.NotEmptyf(t, col.RawType, "it should return the raw type for column %q", col.Name)
						col.RawType = ""
						return col
					})
					require.ElementsMatch(t, actualCols, expectedCols, "it should return the correct columns")
				})
				t.Run("with catalog", func(t *testing.T) {
					table := table
					table.Catalog = currentCatalog
					actualCols, err := legacyDB.ListColumns(ctx, table)
					require.NoError(t, err, "it should be able to list columns")
					actualCols = lo.Map(actualCols, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
						require.NotEmptyf(t, col.RawType, "it should return the raw type for column %q", col.Name)
						col.RawType = ""
						return col
					})
					require.ElementsMatch(t, actualCols, expectedCols, "it should return the correct columns")
				})
			})

			t.Run("list columns for sql query", func(t *testing.T) {
				expectedColsJSON, err := os.ReadFile("testdata/legacy-column-mapping-test-columns-sql.json")
				require.NoErrorf(t, err, "it should be able to read the legacy column mappings json file")
				var expectedColsMap map[string]string
				err = json.Unmarshal(expectedColsJSON, &expectedColsMap)
				require.NoErrorf(t, err, "it should be able to unmarshal the legacy column mappings json file")
				expectedCols := lo.MapToSlice(expectedColsMap, func(k, v string) sqlconnect.ColumnRef {
					return sqlconnect.ColumnRef{Name: k, Type: v}
				})

				actualCols, err := legacyDB.ListColumnsForSqlQuery(ctx, selectSQL)
				require.NoError(t, err, "it should be able to list columns")
				actualCols = lo.Map(actualCols, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
					require.NotEmptyf(t, col.RawType, "it should return the raw type for column %q", col.Name)
					col.RawType = ""
					return col
				})
				require.ElementsMatch(t, actualCols, expectedCols, "it should return the correct columns")
			})

			t.Run("json mapper", func(t *testing.T) {
				expectedRowsJSON, err := os.ReadFile("testdata/legacy-column-mapping-test-rows.json")
				require.NoErrorf(t, err, "it should be able to read the legacy rows json file")

				ch, leave := sqlconnect.QueryJSONAsync(ctx, legacyDB, selectSQL)
				defer leave()
				var rows []any
				for row := range ch {
					require.NoError(t, row.Err, "it should be able to scan a row")
					var o any
					err := json.Unmarshal(row.Value, &o)
					require.NoError(t, err, "it should be able to unmarshal the row")
					rows = append(rows, o)
				}
				actualRowsJSON, err := json.Marshal(rows)
				require.NoError(t, err, "it should be able to marshal the rows")

				require.JSONEq(t, string(expectedRowsJSON), string(actualRowsJSON), "it should return the correct rows: "+string(actualRowsJSON))
			})
		})

		t.Run("async query", func(t *testing.T) {
			t.Run("QueryJSONMapAsync without error", func(t *testing.T) {
				ch, leave := sqlconnect.QueryJSONMapAsync(ctx, db, selectSQL)
				defer leave()
				for row := range ch {
					require.NoError(t, row.Err, "it should be able to scan a row")
				}
			})

			t.Run("QueryJSONMapAsync with context cancelled", func(t *testing.T) {
				ch, leave := sqlconnect.QueryJSONMapAsync(cancelledCtx, db, selectSQL)
				defer leave()
				var iterations int
				for row := range ch {
					iterations++
					require.Error(t, row.Err)
					require.True(t, errors.Is(row.Err, context.Canceled))
				}
				require.Equal(t, 1, iterations, "it should only iterate once")
			})

			t.Run("QueryJSONMapAsync with leave", func(t *testing.T) {
				ch, leave := sqlconnect.QueryJSONMapAsync(cancelledCtx, db, selectSQL)
				leave()
				time.Sleep(10 * time.Millisecond)
				var wg sync.WaitGroup
				var iterations int
				wg.Add(1)
				go func() {
					for range ch {
						iterations++
					}
					wg.Done()
				}()
				wg.Wait()
				require.Equal(t, 0, iterations, "it shouldn't iterate after leaving the channel")
			})
		})
	})

	if opts.ExtraTests != nil {
		t.Run("extra tests", func(t *testing.T) {
			opts.ExtraTests(t, db)
		})
	}
}

func GenerateTestSchema(formatfn func(string) string) string {
	return formatfn(fmt.Sprintf("tsqlcon_%s_%d", rand.String(12), time.Now().Unix()))
}

func ExecuteStatements(t *testing.T, c sqlconnect.DB, schema, path string) {
	for _, stmt := range ReadSQLStatements(t, schema, path) {
		_, err := c.ExecContext(context.Background(), stmt)
		require.NoErrorf(t, err, "it should be able to execute sql statement:\n%s", stmt)
	}
}

func ReadSQLStatements(t *testing.T, schema, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoErrorf(t, err, "it should be able to read the sql script file %q", path)
	tpl, err := template.New("data").Parse(string(data))
	require.NoErrorf(t, err, "it should be able to parse the sql script file %q", path)
	sql := new(strings.Builder)
	templateData := map[string]any{"schema": schema}
	err = tpl.Execute(sql, templateData)
	require.NoErrorf(t, err, "it should be able to execute the sql script file %q", path)
	return sqlconnectutil.SplitStatements(sql.String())
}
