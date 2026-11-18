package mcsql

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRenderLiteral(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{nil, "NULL"},
		{"", "''"},
		{"abc", "'abc'"},
		{"it's", `'it\'s'`},
		{`back\slash`, `'back\\slash'`},
		{`ends in a backslash \`, `'ends in a backslash \\'`},
		{`ends in \'`, `'ends in \\\''`},
		{`it's a "quoted" \ backslash; newline` + "\n", `CONCAT('it\'s a "quoted" \\ backslash; newline', CHR(10))`},
		{"a\x00b", "CONCAT('a', CHR(0), 'b')"},
		{"\t", "CHR(9)"},
		{"\x7f", "CHR(127)"},
		{"中文 ?", "'中文 ?'"},
		{`\u0001`, `'\\u0001'`},
		{true, "TRUE"},
		{false, "FALSE"},
		{0, "0"},
		{int8(-8), "(-8)"},
		{int32(math.MaxInt32), "2147483647"},
		{int64(math.MaxInt32) + 1, "2147483648L"},
		{int64(math.MinInt32) - 1, "(-2147483649L)"},
		{int64(math.MinInt64), "(-9223372036854775808L)"},
		{uint8(7), "7"},
		{uint64(math.MaxInt64), "9223372036854775807L"},
		{1.5, "1.5"},
		{1.0, "1.0"},
		{math.Copysign(0, -1), "(-0.0)"},
		{-2.5, "(-2.5)"},
		{1e21, "1e+21"},
		{1.1, "1.1"},
		{float32(0.1), "0.1"},
		{[]byte{0x00, 0xab}, "UNHEX('00AB')"},
		{[]byte(nil), "NULL"},
	}
	for _, c := range cases {
		got, err := RenderLiteral(c.in)
		require.NoError(t, err, "%#v", c.in)
		require.Equal(t, c.want, got, "%#v", c.in)
	}

	for _, c := range []struct {
		in  any
		err error
	}{
		{math.NaN(), ErrNonFinite},
		{math.Inf(1), ErrNonFinite},
		{math.Inf(-1), ErrNonFinite},
		{uint64(math.MaxUint64), ErrUnsupportedValue},
		{time.Now(), ErrTimeValue},
		{"\xff", ErrInvalidUTF8},
		{struct{ Secret string }{"SENTINEL"}, ErrUnsupportedValue},
		{[]string{"a"}, ErrUnsupportedValue},
	} {
		_, err := RenderLiteral(c.in)
		require.ErrorIs(t, err, c.err, "%#v", c.in)
		require.NotContains(t, err.Error(), "SENTINEL", "the value is never printed")
	}
}

// Every rendered string literal must scan as exactly the tokens of a literal,
// so no value can end the string early or add a statement.
func TestRenderedStringsScanAsOneValue(t *testing.T) {
	for _, s := range []string{
		`'`, `\`, `\'`, `'\`, `''`, `\\'`, "';DROP TABLE t;--", "`", "/*", "*/", "--", "?", "a\nb", "\x00", "\"",
	} {
		lit, err := RenderLiteral(s)
		require.NoError(t, err)
		tokens, err := Scan("SELECT " + lit)
		require.NoError(t, err, s)
		for _, tok := range tokens[1:] {
			require.NotContains(t, []Kind{Semicolon, Placeholder, LineComment, BlockComment, Hint, Quoted}, tok.Kind, "%q rendered as %s", s, lit)
		}
	}
}

func TestBind(t *testing.T) {
	got, err := Bind("SELECT '?', `a?`, ? -- ?\n, /* ? */ ? FROM t WHERE x = ?", []any{1, "it's", nil})
	require.NoError(t, err)
	require.Equal(t, "SELECT '?', `a?`, 1 -- ?\n, /* ? */ 'it\\'s' FROM t WHERE x = NULL", got)

	_, err = Bind("SELECT ?", nil)
	require.ErrorIs(t, err, ErrPlaceholderCount)
	_, err = Bind("SELECT 1", []any{1})
	require.ErrorIs(t, err, ErrPlaceholderCount)
	_, err = Bind("SELECT ?", []any{time.Now()})
	require.ErrorIs(t, err, ErrTimeValue)
	got, err = Bind("SELECT a -? FROM t", []any{-1})
	require.NoError(t, err)
	require.Equal(t, "SELECT a -(-1) FROM t", got, "a negative value cannot start a -- comment")
	got, err = Bind("SELECT 1", nil)
	require.NoError(t, err)
	require.Equal(t, "SELECT 1", got)
}
