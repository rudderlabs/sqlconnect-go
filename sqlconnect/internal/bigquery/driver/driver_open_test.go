package driver_test

import (
	"database/sql"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	_ "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/bigquery/driver"
)

func TestOpenMissingCredentialFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.json")
	q := url.Values{"credential_file": []string{missing}}
	urn := url.URL{Scheme: "bigquery", Host: "test-project", RawQuery: q.Encode()}

	db, err := sql.Open("bigquery", urn.String())
	require.NoError(t, err, "sql.Open should defer connection setup")
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	err = db.Ping()
	require.ErrorContains(t, err, "reading credential file",
		"opening a connection with an unreadable credential file should surface the read error")
}
