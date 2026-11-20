package mcsql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuoting(t *testing.T) {
	require.Equal(t, "`a b`", QuoteIdentifier("a b"))
	require.Equal(t, "`中文`", QuoteIdentifier("中文"))
	require.Equal(t, "`a.b`", QuoteIdentifier("a.b"), "a column name may hold a dot")
	require.Equal(t, RefusedIdentifier, QuoteIdentifier("a`b"))
	require.Equal(t, RefusedIdentifier, QuoteIdentifier("a``b"))
	require.Equal(t, RefusedIdentifier, QuoteIdentifier("a\x00b"))

	require.Equal(t, "`p`.`s`.`t`", QuoteRelation("p", "s", "t"))
	require.Equal(t, "`s`.`t`", QuoteRelation("", "s", "t"))
	require.Equal(t, "`t`", QuoteRelation("", "", "t"))
	require.Equal(t, RefusedIdentifier, QuoteRelation("p", "s", "a.b"))
	require.Equal(t, RefusedIdentifier, QuoteRelation("p", "s`", "t"))
	require.Equal(t, RefusedIdentifier, QuoteRelation("", "", ""))

	require.NoError(t, CheckName("t_12"))
	for _, bad := range []string{"", "a.b", "a`b", "a\x00"} {
		require.ErrorIs(t, CheckName(bad), ErrInvalidName, bad)
	}
	require.Equal(t, "mixedcol", Normalise("MixedCol"))
}
