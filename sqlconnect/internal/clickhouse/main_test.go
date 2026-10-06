package clickhouse_test

import (
	"os"
	"testing"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
)

// TestMain is the only TestMain of the package. It installs the strict
// production policy once; every test that needs another policy builds its DB
// through NewDBForTest or NewDBForTestWith.
func TestMain(m *testing.M) {
	if err := clickhousequery.SetDialPolicy(clickhousequery.DialPolicy{}); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}
