package redshift

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func TestRedshiftListColumns(t *testing.T) {
	stmt, nameCol, typeCol := redshiftListColumns("dev", "test_schema", "test_view")

	require.Equal(t, "column_name", nameCol)
	require.Equal(t, "data_type", typeCol)
	require.Contains(t, stmt, "information_schema.columns")
	require.Contains(t, stmt, "pg_get_late_binding_view_cols()")
	require.Contains(t, stmt, "table_catalog = 'dev'")
	require.Contains(t, stmt, "current_database() = 'dev'")
	require.NotContains(t, strings.ToLower(stmt), "svv_all_columns")
}

func TestRedshiftListColumnsEscapesIdentifiers(t *testing.T) {
	stmt, _, _ := redshiftListColumns(
		base.UnquotedIdentifier("dev'cat"),
		base.UnquotedIdentifier("test'schema"),
		base.UnquotedIdentifier("test'view"),
	)

	require.Contains(t, stmt, "table_catalog = 'dev''cat'")
	require.Contains(t, stmt, "table_schema = 'test''schema'")
	require.Contains(t, stmt, "table_name = 'test''view'")
	require.Contains(t, stmt, "current_database() = 'dev''cat'")
	require.Contains(t, stmt, "view_schema = 'test''schema'")
	require.Contains(t, stmt, "view_name = 'test''view'")
}
