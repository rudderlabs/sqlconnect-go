package maxcompute

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcsql"
)

func TestDialect(t *testing.T) {
	d := newDialect("proj")
	require.Equal(t, "`a b`", d.QuoteIdentifier("a b"))
	require.Equal(t, mcsql.RefusedIdentifier, d.QuoteIdentifier("a`b"))
	require.Equal(t, "`proj`.`s`.`t`", d.QuoteTable(sqlconnect.NewSchemaTableRef("s", "t")), "a two-part ref gets the project")
	require.Equal(t, "`other`.`s`.`t`", d.QuoteTable(sqlconnect.NewRelationRef("t", sqlconnect.WithSchema("s"), sqlconnect.WithCatalog("other"))))
	require.Equal(t, "`t`", d.QuoteTable(sqlconnect.NewRelationRef("t")))
	require.Equal(t, mcsql.RefusedIdentifier, d.QuoteTable(sqlconnect.NewSchemaTableRef("s", "a.b")))
	require.Equal(t, "`s`.`t`", newDialect("").QuoteTable(sqlconnect.NewSchemaTableRef("s", "t")), "the standalone dialect has no project")

	require.Equal(t, "mixed", d.FormatTableName("MiXed"))
	require.Equal(t, "`quoted_column`", d.NormaliseIdentifier("`QuOted_CoLuMn`"), "names fold even inside backticks")

	for in, want := range map[string]sqlconnect.RelationRef{
		"T":                  {Name: "t", Type: sqlconnect.TableRelation},
		"S.T":                {Schema: "s", Name: "t", Type: sqlconnect.TableRelation},
		"`P`.`S`.`T x`":      {Catalog: "p", Schema: "s", Name: "t x", Type: sqlconnect.TableRelation},
		"`s`.UnQuoted_TablE": {Schema: "s", Name: "unquoted_table", Type: sqlconnect.TableRelation},
	} {
		got, err := d.ParseRelationRef(in)
		require.NoError(t, err, in)
		require.Equal(t, want, got, in)
	}
	for _, bad := range []string{"a.b.c.d", "`a``b`"} {
		_, err := d.ParseRelationRef(bad)
		require.Error(t, err, bad)
	}
}

func TestDialectExpressions(t *testing.T) {
	d := newDialect("proj")
	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	cond, err := d.QueryCondition("_timestamp", "eq", ts)
	require.NoError(t, err)
	require.Equal(t, "(_timestamp = FROM_UTC_TIMESTAMP(1609459200000L, 'UTC'))", cond.String(), "a time is an epoch instant, exact in every zone")

	cond, err = d.QueryCondition("_string", "eq", `it's \ x`)
	require.NoError(t, err)
	require.Equal(t, "(_string = 'it\\'s \\\\ x')", cond.String(), "strings are backslash-escaped, never ''-doubled")

	cond, err = d.QueryCondition("_boolean", "eq", true)
	require.NoError(t, err)
	require.Contains(t, cond.String(), "TRUE")

	add, err := d.Expressions().TimestampAdd(ts, 1, "hour")
	require.NoError(t, err)
	require.Equal(t, "DATEADD(FROM_UTC_TIMESTAMP(1609459200000L, 'UTC'), 1, 'hh')", add.String())
	add, err = d.Expressions().TimestampAdd("CURRENT_TIMESTAMP", -1, "day")
	require.NoError(t, err)
	require.Equal(t, "DATEADD(CURRENT_TIMESTAMP(), -1, 'dd')", add.String())
	add, err = d.Expressions().DateAdd("CURRENT_TIMESTAMP", 2, "week")
	require.NoError(t, err)
	require.Equal(t, "DATEADD(DATE(CURRENT_TIMESTAMP()), 14, 'dd')", add.String())
	cond, err = d.QueryCondition("DATE(_timestamp)", "inlast", 1, "day")
	require.NoError(t, err)
	require.Contains(t, cond.String(), "DATEADD(DATE(CURRENT_DATE()), -1, 'dd')")
}

func TestMappings(t *testing.T) {
	for raw, want := range map[string]string{
		"TINYINT": "int", "BIGINT": "int", "DECIMAL(22,6)": "float", "DOUBLE": "float", "VARCHAR(255)": "string",
		"CHAR(3)": "string", "BINARY": "string", "BOOLEAN": "boolean", "TIMESTAMP_NTZ": "datetime", "DATE": "datetime",
		"ARRAY<INT>": "array", "MAP<STRING,INT>": "json", "STRUCT<`a`:INT>": "json", "JSON": "json",
		"INTERVAL_DAY_TIME": "unsupported",
	} {
		require.Equal(t, want, canonicalType(raw), raw)
	}
	require.Equal(t, json.Number("1.10"), jsonRowMapper("DECIMAL(10,2)", "1.10"))
	require.Equal(t, "AAE=", jsonRowMapper("BINARY", []byte{0, 1}))
	require.Equal(t, json.RawMessage(`{"a":1}`), jsonRowMapper("STRUCT<`a`:INT>", []byte(`{"a":1}`)))
	require.Nil(t, jsonRowMapper("STRING", nil))
}
