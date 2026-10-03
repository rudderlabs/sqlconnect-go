package chsql_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chsql"
)

func TestLexer_States(t *testing.T) {
	toks, err := chsql.Tokenize("SELECT 'it\\'s FINAL', `SAMPLE` /* FORMAT /* nested */ x */ -- SETTINGS\nFROM t WHERE t = ['a', 'b']")
	require.NoError(t, err)
	var words []string
	for _, tk := range toks {
		if tk.Kind == chsql.Word {
			words = append(words, tk.Upper)
		}
		if tk.Text == "," && tk.Kind == chsql.Punct && tk.Pos > 40 {
			require.Equal(t, 1, tk.Depth, "a comma inside [] is nested")
		}
	}
	require.Equal(t, []string{"SELECT", "FROM", "T", "WHERE", "T"}, words)
	for _, bad := range []string{"SELECT 'unterminated", "SELECT 1 /* open"} {
		_, err = chsql.Tokenize(bad)
		require.Error(t, err, bad)
	}
	out, err := chsql.TrimTerminalSemicolon("SELECT 1 ;  \n")
	require.NoError(t, err)
	require.Equal(t, "SELECT 1", out)
	for _, bad := range []string{"SELECT 1; -- tail", "SELECT 1; SELECT 2", "SELECT 1;;"} { // anything after a semicolon is a second statement
		_, err = chsql.TrimTerminalSemicolon(bad)
		require.Error(t, err, bad)
	}
}

// words returns the upper-case Word tokens of sql.
func words(t *testing.T, sql string) []string {
	t.Helper()
	toks, err := chsql.Tokenize(sql)
	require.NoError(t, err, sql)
	var out []string
	for _, tk := range toks {
		if tk.Kind == chsql.Word {
			out = append(out, tk.Upper)
		}
	}
	return out
}

// TestLexer_ClickHouseStates pins the token states of the ClickHouse 26.3 lexer
// (src/Parsers/Lexer.cpp) that can hide a quote or a keyword from a scanner
// that does not know them.
func TestLexer_ClickHouseStates(t *testing.T) {
	for name, tc := range map[string]struct {
		sql   string
		words []string
	}{
		"double-slash comment hides its quote":   {"SELECT 1 // it's\nFROM url", []string{"SELECT", "FROM", "URL"}},
		"doubled quote inside a string":          {"SELECT 'a''FROM' FROM t", []string{"SELECT", "FROM", "T"}},
		"doubled backquote in an identifier":     {"SELECT `a``FROM` FROM t", []string{"SELECT", "FROM", "T"}},
		"escaped double quote in an identifier":  {`SELECT "a\"FROM" FROM t`, []string{"SELECT", "FROM", "T"}},
		"heredoc hides its quote":                {"SELECT $$ it's $$ FROM url", []string{"SELECT", "FROM", "URL"}},
		"named heredoc":                          {"SELECT $x$ FROM $ $x$ FROM t", []string{"SELECT", "FROM", "T"}},
		"heredoc tag must be word characters":    {"SELECT $ a$ FROM t", []string{"SELECT", "A$", "FROM", "T"}},
		"dollar inside a bare word":              {"SELECT a$b$ FROM t", []string{"SELECT", "A$B$", "FROM", "T"}},
		"unclosed heredoc is a dollar sign":      {"SELECT $$ FROM t", []string{"SELECT", "FROM", "T"}},
		"unicode single quotes are a string":     {"SELECT ‘'’ FROM url", []string{"SELECT", "FROM", "URL"}},
		"unicode double quotes are an ident":     {"SELECT “'” FROM url", []string{"SELECT", "FROM", "URL"}},
		"no-break space separates words":         {"SELECT 1 FROM url", []string{"SELECT", "FROM", "URL"}},
		"ideographic space separates words":      {"SELECT 1 FROM　url", []string{"SELECT", "FROM", "URL"}},
		"exponent sign is part of the number":    {"SELECT 1e--x FROM url", []string{"SELECT", "X", "FROM", "URL"}},
		"leading-dot number exponent":            {"SELECT (.5e--x) FROM url", []string{"SELECT", "X", "FROM", "URL"}},
		"dot after a word is tuple access":       {"SELECT .5e--x FROM url", []string{"SELECT", "5E"}},
		"number glued to a word is a bare word":  {"SELECT 1abc FROM t", []string{"SELECT", "1ABC", "FROM", "T"}},
		"hex string literal":                     {"SELECT x'4142' FROM t", []string{"SELECT", "FROM", "T"}},
		"tuple access after a dot is an integer": {"SELECT t.1.1 FROM t", []string{"SELECT", "T", "FROM", "T"}},
		"hash comment needs a space":             {"SELECT 1 # it's\nFROM t", []string{"SELECT", "FROM", "T"}},
		"math minus is punctuation":              {"SELECT 1−2 FROM t", []string{"SELECT", "FROM", "T"}},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.words, words(t, tc.sql))
		})
	}
}

// TestLexer_ServerRejects covers text the ClickHouse lexer turns into an error
// token. Tokenize fails on it too, so no caller scans past it.
func TestLexer_ServerRejects(t *testing.T) {
	for _, bad := range []string{
		"SELECT 1 #x", "SELECT !x", `SELECT 1 \G`, "SELECT x'4G'", "SELECT b'012'",
		"SELECT 1.5abc", "SELECT ‘open", "SELECT café", "SELECT 'a\\",
		"SELECT `open", `SELECT "open`,
	} {
		_, err := chsql.Tokenize(bad)
		require.Error(t, err, bad)
	}
}

func TestLexer_Positions(t *testing.T) {
	sql := "SELECT (a, [b]) -- c\n"
	toks, err := chsql.Tokenize(sql)
	require.NoError(t, err)
	var rebuilt strings.Builder
	for _, tk := range toks {
		require.Equal(t, tk.Text, sql[tk.Pos:tk.Pos+len(tk.Text)])
		rebuilt.WriteString(tk.Text)
	}
	require.Equal(t, sql, rebuilt.String())
	depth := map[string]int{}
	for _, tk := range toks {
		if tk.Kind == chsql.Punct {
			depth[tk.Text] = tk.Depth
		}
	}
	require.Equal(t, map[string]int{"(": 1, ",": 1, "[": 2, "]": 2, ")": 1}, depth)
}

func TestTrimTerminalSemicolon_Cases(t *testing.T) {
	for in, want := range map[string]string{
		"SELECT 1":               "SELECT 1",
		"SELECT 1 ; ":            "SELECT 1",
		"SELECT ';' FROM t;":     "SELECT ';' FROM t",
		"SELECT 1 -- c\n;\n":     "SELECT 1 -- c",
		"SELECT 1 /* ; */":       "SELECT 1 /* ; */",
		"SELECT $$;$$ FROM t ; ": "SELECT $$;$$ FROM t",
		"SELECT 1\ufeff;":        "SELECT 1",
		"SELECT 1\u200b;\u200b":  "SELECT 1",
		"SELECT 1\ufeff":         "SELECT 1",
		"SELECT 1\u200b":         "SELECT 1",
		" ; ":                    "",
	} {
		got, err := chsql.TrimTerminalSemicolon(in)
		require.NoError(t, err, in)
		require.Equal(t, want, got, in)
	}
	for _, bad := range []string{"SELECT 1; /* c */", "; SELECT 1", "SELECT 1 // c ;\n; x", "SELECT 'open;"} {
		_, err := chsql.TrimTerminalSemicolon(bad)
		require.Error(t, err, bad)
	}
}
