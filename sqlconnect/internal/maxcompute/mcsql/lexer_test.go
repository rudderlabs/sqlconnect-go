package mcsql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func kinds(t *testing.T, sql string) []Kind {
	t.Helper()
	tokens, err := Scan(sql)
	require.NoError(t, err)
	out := make([]Kind, len(tokens))
	for i, tok := range tokens {
		out[i] = tok.Kind
	}
	return out
}

func TestScan(t *testing.T) {
	require.Equal(t, []Kind{Word, Word, Placeholder}, kinds(t, "select a ?"))
	require.Equal(t, []Kind{Word, String, Punct, String}, kinds(t, `select 'a;?' , "b'?;"`))
	require.Equal(t, []Kind{Word, String}, kinds(t, `select 'it\'s ? ;'`), "a backslash escapes the quote")
	require.Equal(t, []Kind{Word, String}, kinds(t, `select 'ends in a backslash \\'`))
	require.Equal(t, []Kind{Word, String, String}, kinds(t, `select 'it''s'`), "'' is two adjacent literals")
	require.Equal(t, []Kind{Word, Quoted}, kinds(t, "select `a;?'b`"))
	require.Equal(t, []Kind{Word, Word, LineComment, Word}, kinds(t, "select a -- x; ? 'y\nfrom"))
	require.Equal(t, []Kind{Word, BlockComment, Word}, kinds(t, "select /* ; ? ' */ a"))
	require.Equal(t, []Kind{Word, Hint, Word}, kinds(t, "select /*+ mapjoin(a) */ a"))
	require.Equal(t, []Kind{Punct, Word, Punct, Punct, Word}, kinds(t, "@v := select"))

	for _, bad := range []string{"select 'abc", `select "abc`, "select `abc", "select /* abc", `select 'abc\'`} {
		_, err := Scan(bad)
		require.Error(t, err, bad)
	}
}

func TestStatements(t *testing.T) {
	count := func(sql string) int {
		tokens, err := Scan(sql)
		require.NoError(t, err)
		return len(Statements(tokens))
	}
	require.Equal(t, 1, count("select 1"))
	require.Equal(t, 1, count("select 1;"))
	require.Equal(t, 1, count("select 1; -- done"))
	require.Equal(t, 1, count("select 1; /* done */ ;"))
	require.Equal(t, 1, count("select ';' ; "))
	require.Equal(t, 1, count("select `a;b`"))
	require.Equal(t, 2, count("select 1; select 2"))
	require.Equal(t, 2, count("set a=b; select 2;"))
	require.Equal(t, 0, count("-- nothing"))
}

func TestTerminate(t *testing.T) {
	term := func(sql string) string {
		tokens, err := Scan(sql)
		require.NoError(t, err)
		return Terminate(sql, tokens)
	}
	require.Equal(t, "select 1;", term("select 1"))
	require.Equal(t, "select 1;", term("select 1  \n"))
	require.Equal(t, "select 1;", term("select 1;"))
	require.Equal(t, "select 1; -- x", term("select 1; -- x"))
	require.Equal(t, "select 1 -- x\n;", term("select 1 -- x"))
	require.Equal(t, "select 1 /* x */;", term("select 1 /* x */"))
	require.Equal(t, "select ';';", term("select ';'"))
}
