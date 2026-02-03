package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestDialectFactory(t *testing.T) {
	t.Run("postgres dialect factory is registered", func(t *testing.T) {
		dialect, err := sqlconnect.NewDialect("postgres", nil)
		require.NoError(t, err, "should create postgres dialect without error")
		require.NotNil(t, dialect, "dialect should not be nil")

		// Test basic functionality
		quoted := dialect.QuoteIdentifier("test_column")
		require.Equal(t, `"test_column"`, quoted, "should quote identifier correctly")

		formatted := dialect.FormatTableName("TestTable")
		require.Equal(t, "testtable", formatted, "should format table name correctly")
	})
}
