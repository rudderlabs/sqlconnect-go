package fabric

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestDialect(t *testing.T) {
	d := newDialect()
	require.Equal(t, "[na]]me]", d.QuoteIdentifier("na]me"))
	require.Equal(t, "[Catalog].[Schema].[Ta]]ble]", d.QuoteTable(sqlconnect.RelationRef{Catalog: "Catalog", Schema: "Schema", Name: "Ta]ble"}))
	require.Equal(t, `Catalog.[Sch.Ema].[Ta]]ble]`, d.NormaliseIdentifier(`Catalog.[Sch.Ema].[Ta]]ble]`))

	ref, err := d.ParseRelationRef(`[Cat.A]]log].[Sch.Ema].[Ta]]ble]`)
	require.NoError(t, err)
	require.Equal(t, sqlconnect.RelationRef{Catalog: "Cat.A]log", Schema: "Sch.Ema", Name: "Ta]ble"}, ref)
}
