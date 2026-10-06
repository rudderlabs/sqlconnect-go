package fabric

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestDialect(t *testing.T) {
	d := newDialect()
	require.Equal(t, `"na""me"`, d.QuoteIdentifier(`na"me`))
	require.Equal(t, `"Catalog"."Schema"."Ta""ble"`, d.QuoteTable(sqlconnect.RelationRef{Catalog: "Catalog", Schema: "Schema", Name: `Ta"ble`}))
	require.Equal(t, `Catalog."SchEma"."Ta""ble"`, d.NormaliseIdentifier(`Catalog."SchEma"."Ta""ble"`))

	ref, err := d.ParseRelationRef(`"Cat""alog"."SchEma"."Ta""ble"`)
	require.NoError(t, err)
	require.Equal(t, sqlconnect.RelationRef{Catalog: `Cat"alog`, Schema: "SchEma", Name: `Ta"ble`}, ref)
}
