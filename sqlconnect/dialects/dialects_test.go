package dialects_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/dialects"
)

func TestNewDialect(t *testing.T) {
	tests := []struct {
		name          string
		warehouseType string
		wantErr       bool
	}{
		{name: "snowflake", warehouseType: "snowflake", wantErr: false},
		{name: "bigquery", warehouseType: "bigquery", wantErr: false},
		{name: "redshift", warehouseType: "redshift", wantErr: false},
		{name: "postgres", warehouseType: "postgres", wantErr: false},
		{name: "databricks", warehouseType: "databricks", wantErr: false},
		{name: "mysql", warehouseType: "mysql", wantErr: false},
		{name: "trino", warehouseType: "trino", wantErr: false},
		{name: "unknown", warehouseType: "unknown", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialect, err := dialects.NewDialect(tt.warehouseType)
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, dialect)
			} else {
				require.NoError(t, err)
				require.NotNil(t, dialect)
			}
		})
	}
}

func TestDialectNormaliseIdentifier(t *testing.T) {
	tests := []struct {
		warehouseType string
		identifier    string
		expected      string
	}{
		// Snowflake: uppercase unquoted identifiers
		{warehouseType: "snowflake", identifier: "users", expected: "USERS"},
		{warehouseType: "snowflake", identifier: "user_id", expected: "USER_ID"},
		{warehouseType: "snowflake", identifier: `"MixedCase"`, expected: `"MixedCase"`},
		{warehouseType: "snowflake", identifier: "schema.table", expected: "SCHEMA.TABLE"},

		// PostgreSQL: lowercase unquoted identifiers
		{warehouseType: "postgres", identifier: "USERS", expected: "users"},
		{warehouseType: "postgres", identifier: "User_ID", expected: "user_id"},
		{warehouseType: "postgres", identifier: `"MixedCase"`, expected: `"MixedCase"`},
		{warehouseType: "postgres", identifier: "SCHEMA.TABLE", expected: "schema.table"},

		// BigQuery: case-sensitive (no transformation for unquoted)
		{warehouseType: "bigquery", identifier: "users", expected: "users"},
		{warehouseType: "bigquery", identifier: "Users", expected: "Users"},

		// Redshift (default, case-insensitive): lowercase all
		{warehouseType: "redshift", identifier: "USERS", expected: "users"},
		{warehouseType: "redshift", identifier: "User_ID", expected: "user_id"},

		// Databricks: lowercase
		{warehouseType: "databricks", identifier: "USERS", expected: "users"},
		{warehouseType: "databricks", identifier: "User_ID", expected: "user_id"},

		// Trino: lowercase
		{warehouseType: "trino", identifier: "USERS", expected: "users"},
		{warehouseType: "trino", identifier: "User_ID", expected: "user_id"},

		// MySQL: case-sensitive (identity function)
		{warehouseType: "mysql", identifier: "users", expected: "users"},
		{warehouseType: "mysql", identifier: "Users", expected: "Users"},
	}

	for _, tt := range tests {
		t.Run(tt.warehouseType+"_"+tt.identifier, func(t *testing.T) {
			dialect, err := dialects.NewDialect(tt.warehouseType)
			require.NoError(t, err)

			result := dialect.NormaliseIdentifier(tt.identifier)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDialectQuoteIdentifier(t *testing.T) {
	tests := []struct {
		warehouseType string
		identifier    string
		expected      string
	}{
		// Snowflake: double quotes
		{warehouseType: "snowflake", identifier: "users", expected: `"users"`},
		{warehouseType: "snowflake", identifier: "user_id", expected: `"user_id"`},

		// PostgreSQL: double quotes
		{warehouseType: "postgres", identifier: "users", expected: `"users"`},
		{warehouseType: "postgres", identifier: "user_id", expected: `"user_id"`},

		// BigQuery: backticks
		{warehouseType: "bigquery", identifier: "users", expected: "`users`"},
		{warehouseType: "bigquery", identifier: "user_id", expected: "`user_id`"},

		// Redshift: double quotes
		{warehouseType: "redshift", identifier: "users", expected: `"users"`},
		{warehouseType: "redshift", identifier: "user_id", expected: `"user_id"`},

		// Databricks: backticks
		{warehouseType: "databricks", identifier: "users", expected: "`users`"},
		{warehouseType: "databricks", identifier: "user_id", expected: "`user_id`"},

		// Trino: double quotes
		{warehouseType: "trino", identifier: "users", expected: `"users"`},
		{warehouseType: "trino", identifier: "user_id", expected: `"user_id"`},

		// MySQL: backticks
		{warehouseType: "mysql", identifier: "users", expected: "`users`"},
		{warehouseType: "mysql", identifier: "user_id", expected: "`user_id`"},
	}

	for _, tt := range tests {
		t.Run(tt.warehouseType+"_"+tt.identifier, func(t *testing.T) {
			dialect, err := dialects.NewDialect(tt.warehouseType)
			require.NoError(t, err)

			result := dialect.QuoteIdentifier(tt.identifier)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDialectQuoteTable(t *testing.T) {
	tests := []struct {
		warehouseType string
		table         sqlconnect.RelationRef
		expected      string
	}{
		// Snowflake
		{warehouseType: "snowflake", table: sqlconnect.NewRelationRef("users"), expected: `"users"`},
		{warehouseType: "snowflake", table: sqlconnect.NewRelationRef("users", sqlconnect.WithSchema("public")), expected: `"public"."users"`},

		// PostgreSQL
		{warehouseType: "postgres", table: sqlconnect.NewRelationRef("users"), expected: `"users"`},
		{warehouseType: "postgres", table: sqlconnect.NewRelationRef("users", sqlconnect.WithSchema("public")), expected: `"public"."users"`},

		// BigQuery (schema.table in single backticks)
		{warehouseType: "bigquery", table: sqlconnect.NewRelationRef("users"), expected: "`users`"},
		{warehouseType: "bigquery", table: sqlconnect.NewRelationRef("users", sqlconnect.WithSchema("dataset")), expected: "`dataset.users`"},

		// Redshift
		{warehouseType: "redshift", table: sqlconnect.NewRelationRef("users"), expected: `"users"`},
		{warehouseType: "redshift", table: sqlconnect.NewRelationRef("users", sqlconnect.WithSchema("public")), expected: `"public"."users"`},

		// Databricks
		{warehouseType: "databricks", table: sqlconnect.NewRelationRef("users"), expected: "`users`"},
		{warehouseType: "databricks", table: sqlconnect.NewRelationRef("users", sqlconnect.WithSchema("default")), expected: "`default`.`users`"},

		// Trino
		{warehouseType: "trino", table: sqlconnect.NewRelationRef("users"), expected: `"users"`},
		{warehouseType: "trino", table: sqlconnect.NewRelationRef("users", sqlconnect.WithSchema("public")), expected: `"public"."users"`},

		// MySQL
		{warehouseType: "mysql", table: sqlconnect.NewRelationRef("users"), expected: "`users`"},
		{warehouseType: "mysql", table: sqlconnect.NewRelationRef("users", sqlconnect.WithSchema("mydb")), expected: "`mydb`.`users`"},
	}

	for _, tt := range tests {
		t.Run(tt.warehouseType+"_"+tt.table.String(), func(t *testing.T) {
			dialect, err := dialects.NewDialect(tt.warehouseType)
			require.NoError(t, err)

			result := dialect.QuoteTable(tt.table)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDialectParseRelationRef(t *testing.T) {
	tests := []struct {
		warehouseType string
		identifier    string
		expected      sqlconnect.RelationRef
	}{
		// Snowflake: uppercase normalization
		{warehouseType: "snowflake", identifier: "users", expected: sqlconnect.RelationRef{Name: "USERS"}},
		{warehouseType: "snowflake", identifier: "public.users", expected: sqlconnect.RelationRef{Schema: "PUBLIC", Name: "USERS"}},
		{warehouseType: "snowflake", identifier: `"MixedCase"`, expected: sqlconnect.RelationRef{Name: "MixedCase"}},

		// PostgreSQL: lowercase normalization
		{warehouseType: "postgres", identifier: "USERS", expected: sqlconnect.RelationRef{Name: "users"}},
		{warehouseType: "postgres", identifier: "PUBLIC.USERS", expected: sqlconnect.RelationRef{Schema: "public", Name: "users"}},
		{warehouseType: "postgres", identifier: `"MixedCase"`, expected: sqlconnect.RelationRef{Name: "MixedCase"}},

		// Redshift: lowercase
		{warehouseType: "redshift", identifier: "USERS", expected: sqlconnect.RelationRef{Name: "users"}},
		{warehouseType: "redshift", identifier: "PUBLIC.USERS", expected: sqlconnect.RelationRef{Schema: "public", Name: "users"}},

		// Databricks: lowercase
		{warehouseType: "databricks", identifier: "USERS", expected: sqlconnect.RelationRef{Name: "users"}},
		{warehouseType: "databricks", identifier: "DEFAULT.USERS", expected: sqlconnect.RelationRef{Schema: "default", Name: "users"}},
	}

	for _, tt := range tests {
		t.Run(tt.warehouseType+"_"+tt.identifier, func(t *testing.T) {
			dialect, err := dialects.NewDialect(tt.warehouseType)
			require.NoError(t, err)

			result, err := dialect.ParseRelationRef(tt.identifier)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDialectQueryCondition(t *testing.T) {
	// Test that QueryCondition works via NewDialect()
	// This verifies the full Dialect interface is properly implemented
	warehouses := []string{"snowflake", "bigquery", "redshift", "postgres", "databricks", "mysql", "trino"}

	for _, wh := range warehouses {
		t.Run(wh, func(t *testing.T) {
			dialect, err := dialects.NewDialect(wh)
			require.NoError(t, err)

			// Test eq operator
			expr, err := dialect.QueryCondition("status", "eq", "active")
			require.NoError(t, err)
			require.NotNil(t, expr)
			require.NotEmpty(t, expr.String())

			// Test in operator
			expr, err = dialect.QueryCondition("id", "in", 1, 2, 3)
			require.NoError(t, err)
			require.NotNil(t, expr)
			require.NotEmpty(t, expr.String())

			// Test null operator
			expr, err = dialect.QueryCondition("deleted_at", "null")
			require.NoError(t, err)
			require.NotNil(t, expr)
			require.NotEmpty(t, expr.String())
		})
	}
}

func TestDialectExpressions(t *testing.T) {
	// Test that Expressions() (DateAdd, TimestampAdd) works via NewDialect()
	warehouses := []string{"snowflake", "bigquery", "redshift", "postgres", "databricks", "mysql", "trino"}

	for _, wh := range warehouses {
		t.Run(wh, func(t *testing.T) {
			dialect, err := dialects.NewDialect(wh)
			require.NoError(t, err)

			expressions := dialect.Expressions()
			require.NotNil(t, expressions)

			// Test DateAdd
			dateExpr, err := expressions.DateAdd("CURRENT_DATE", -30, "day")
			require.NoError(t, err)
			require.NotNil(t, dateExpr)
			require.NotEmpty(t, dateExpr.String())

			// Test TimestampAdd
			tsExpr, err := expressions.TimestampAdd("CURRENT_TIMESTAMP", -1, "hour")
			require.NoError(t, err)
			require.NotNil(t, tsExpr)
			require.NotEmpty(t, tsExpr.String())

			// Test Literal
			litExpr, err := expressions.Literal("COUNT(*)")
			require.NoError(t, err)
			require.NotNil(t, litExpr)
			require.Equal(t, "COUNT(*)", litExpr.String())
		})
	}
}

func TestNewRedshiftDialectWithOptions(t *testing.T) {
	t.Run("case insensitive (default)", func(t *testing.T) {
		dialect := dialects.NewRedshiftDialectWithOptions(false)
		require.NotNil(t, dialect)

		// With case insensitive, all identifiers are lowercased
		result := dialect.NormaliseIdentifier("USERS")
		require.Equal(t, "users", result)

		result = dialect.NormaliseIdentifier(`"MixedCase"`)
		require.Equal(t, `"mixedcase"`, result)
	})

	t.Run("case sensitive", func(t *testing.T) {
		dialect := dialects.NewRedshiftDialectWithOptions(true)
		require.NotNil(t, dialect)

		// With case sensitive, quoted identifiers preserve case
		result := dialect.NormaliseIdentifier("USERS")
		require.Equal(t, "users", result) // Unquoted still lowercased

		result = dialect.NormaliseIdentifier(`"MixedCase"`)
		require.Equal(t, `"MixedCase"`, result) // Quoted preserves case
	})

	t.Run("quoting works", func(t *testing.T) {
		dialect := dialects.NewRedshiftDialectWithOptions(false)

		quoted := dialect.QuoteIdentifier("user_id")
		require.Equal(t, `"user_id"`, quoted)

		table := sqlconnect.NewRelationRef("users", sqlconnect.WithSchema("public"))
		quotedTable := dialect.QuoteTable(table)
		require.Equal(t, `"public"."users"`, quotedTable)
	})
}
