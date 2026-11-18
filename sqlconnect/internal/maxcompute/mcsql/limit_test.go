package mcsql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLimitZero(t *testing.T) {
	for in, want := range map[string]string{
		"select * from (select a from t) sq limit 1": "select * from (select a from t) sq limit 1",
		"SELECT a FROM t LIMIT 5;":                   "SELECT a FROM t LIMIT 5;",
		"SELECT a FROM t":                            "SELECT * FROM (\nSELECT a FROM t\n) sq LIMIT 0",
		"SELECT a FROM (SELECT b FROM u LIMIT 1) x;": "SELECT * FROM (\nSELECT a FROM (SELECT b FROM u LIMIT 1) x\n) sq LIMIT 0",
		"SELECT 'limit' AS a -- limit":               "SELECT * FROM (\nSELECT 'limit' AS a -- limit\n) sq LIMIT 0",
		"SELECT `limit` FROM t":                      "SELECT * FROM (\nSELECT `limit` FROM t\n) sq LIMIT 0",
	} {
		got, err := LimitZero(in)
		require.NoError(t, err)
		require.Equal(t, want, got, in)
	}
}
