package clickhouse

import "testing"

func TestMetadataQueryBoundary(t *testing.T) {
	t.Parallel()
	for _, query := range []string{"SELECT 1", "SELECT 1;", "WITH a AS (SELECT 1) SELECT * FROM a", "SELECT ';'", "SELECT 1 -- comment", "/* comment */ SELECT 1; -- end", "SELECT `format` FROM t"} {
		if _, err := selectQuery(query); err != nil {
			t.Errorf("rejected %q: %v", query, err)
		}
	}
	for _, query := range []string{"", "SELECT 1; SELECT 2", "SELECT 1;;", "SELECT 1 FORMAT JSON", "INSERT INTO t SELECT 1", "SELECT 'open", "SELECT (1", "SELECT 1 /* open"} {
		if _, err := selectQuery(query); err == nil {
			t.Errorf("accepted %q", query)
		}
	}
}
