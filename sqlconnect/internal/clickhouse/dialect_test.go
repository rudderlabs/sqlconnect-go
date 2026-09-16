package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestDialectIdentifiers(t *testing.T) {
	t.Parallel()
	d := newDialect()
	for _, name := range []string{"Events", "a.b", "a`b", `a\b`, "日本語", "a b", "O'Reilly", `a"b`} {
		quoted := d.QuoteIdentifier(name)
		ref, err := d.ParseRelationRef(quoted)
		require.NoError(t, err)
		require.Equal(t, name, ref.Name)
		require.Equal(t, name, d.NormaliseIdentifier(name))
	}
	ref, err := d.ParseRelationRef("`Analytics`.`Events`")
	require.NoError(t, err)
	require.Equal(t, sqlconnect.RelationRef{Schema: "Analytics", Name: "Events"}, ref)
	ref, err = d.ParseRelationRef(`"db.with.dot"."t.dot"`)
	require.NoError(t, err)
	require.Equal(t, "db.with.dot", ref.Schema)
	require.Equal(t, "t.dot", ref.Name)
	require.Equal(t, "`a\\\\b\\`c`", d.QuoteIdentifier("a\\b`c"))
	for _, input := range []string{"", "a.b.c", "a..b", "a.", "`open", "a b", "`a`junk", ".a"} {
		_, err := d.ParseRelationRef(input)
		require.Error(t, err, input)
	}
	expression, err := d.QueryCondition("a`b", "eq", "O'Reilly\\test")
	require.NoError(t, err)
	require.Contains(t, expression.String(), "`a\\`b`")
}
