package mcsql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckModelSQL(t *testing.T) {
	accept := []string{
		"SELECT 1",
		"select 1;",
		"select 1;  -- done",
		"  -- a comment\nSELECT a FROM s.t",
		"/* lead */ SELECT a FROM t",
		"(SELECT a FROM t) UNION ALL (SELECT b FROM u)",
		"WITH x AS (SELECT 1 AS a) SELECT * FROM x",
		"SELECT 'it\\'s; DROP TABLE t; --' AS a",
		"SELECT \"insert; delete\" AS a",
		"SELECT `insert`, `update`, `select;` FROM t",
		"SELECT t.update, t.create FROM t",
		"SELECT a FROM t WHERE b = '/*+ mapjoin(t) */'",
		"SELECT a -- ; DROP TABLE t\nFROM t",
		"SELECT a /* ; */ FROM t",
		"SELECT * FROM t WHERE email LIKE '%@example.com'",
		"select max(updated_at) as last_updated from s.t",
	}
	for _, sql := range accept {
		require.NoError(t, CheckModelSQL(sql), sql)
	}

	refuse := []string{
		"",
		"-- only a comment",
		"SET odps.sql.allow.fullscan=true; SELECT 1",
		"SET odps.sql.allow.fullscan=true",
		"SELECT 1; SELECT 2",
		"SELECT 1;;",
		"SELECT 1; -- x\n; ",
		"select 1 ; drop table t",
		"ADD JAR x.jar",
		"CREATE TABLE x AS SELECT 1",
		"DROP TABLE t",
		"ALTER TABLE t RENAME TO u",
		"INSERT INTO t SELECT 1",
		"DELETE FROM t",
		"UPDATE t SET a = 1",
		"MERGE INTO t USING u ON t.a = u.a WHEN MATCHED THEN DELETE",
		"TRUNCATE TABLE t",
		"GRANT Select ON TABLE t TO USER x",
		"REVOKE Select ON TABLE t FROM USER x",
		"INSTALL PACKAGE x",
		"EXPLAIN SELECT 1",
		"COST SQL SELECT 1",
		"@a := SELECT 1",
		"SELECT * FROM t WHERE a = @a",
		"WITH x AS (SELECT 1 AS a) INSERT INTO t SELECT * FROM x",
		"WITH x AS (SELECT 1 AS a) INSERT OVERWRITE TABLE t SELECT * FROM x",
		"FROM t INSERT INTO u SELECT a",
		"SELECT /*+ mapjoin(b) */ a FROM t",
		"SELECT 'unterminated",
		"SELECT `unterminated",
		"SELECT /* unterminated",
		"SELECT 'it\\'s' ; DROP TABLE t",
		"SELECT 1; -- hide\rDROP TABLE sensitive;",
		"SELECT 1 -- hide\rUNION ALL SELECT 2; DROP TABLE t",
	}
	for _, sql := range refuse {
		err := CheckModelSQL(sql)
		require.ErrorIs(t, err, ErrModelSQL, sql)
		require.NotContains(t, err.Error(), "odps.sql", "the error does not echo the SQL")
	}
}
