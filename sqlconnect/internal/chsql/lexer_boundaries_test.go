package chsql_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chsql"
)

func TestNumberTokenKinds(t *testing.T) {
	for _, tc := range []struct {
		text string
		kind chsql.TokenKind
	}{
		{"0", chsql.Number},
		{"9", chsql.Number},
		{"0xAf", chsql.Number},
		{"0b10", chsql.Number},
		{"1_000", chsql.Number},
		{".5", chsql.Number},
		{"1e-5", chsql.Number},
		{"0x1p-2", chsql.Number},
		{"0x", chsql.Word},
		{"0b2", chsql.Word},
		{"1abc", chsql.Word},
		{"1__0", chsql.Word},
	} {
		t.Run(tc.text, func(t *testing.T) {
			tokens, err := chsql.Tokenize(tc.text)
			require.NoError(t, err)
			require.Len(t, tokens, 1)
			require.Equal(t, tc.kind, tokens[0].Kind)
			require.Equal(t, tc.text, tokens[0].Text)
		})
	}
}

func TestUTF8WhitespaceBoundaries(t *testing.T) {
	// ClickHouse 26.3 skipWhitespacesUTF8 includes zero-width separators; Go's
	// and JavaScript's general-purpose Unicode whitespace sets do not match it.
	spaces := []rune{0x85, 0xa0, 0x180e, 0x2028, 0x2029, 0x202f, 0x205f, 0x2060, 0x3000, 0xfeff}
	for r := rune(0x2000); r <= 0x200d; r++ {
		spaces = append(spaces, r)
	}
	for _, r := range spaces {
		t.Run(fmt.Sprintf("U+%04X", r), func(t *testing.T) {
			for _, tail := range []string{"", "SELECT 0"} {
				tokens, err := chsql.Tokenize(string(r) + tail)
				require.NoError(t, err)
				require.NotEmpty(t, tokens)
				require.Equal(t, chsql.Space, tokens[0].Kind)
				require.Equal(t, string(r), tokens[0].Text)
			}
		})
	}
	for _, r := range []rune{0x84, 0x86, 0x1680, 0x180d, 0x180f, 0x200e, 0x2027, 0x202a, 0x2030, 0x205e, 0x2061, 0x2fff, 0x3001, 0xfffe} {
		_, err := chsql.Tokenize(string(r) + "SELECT 0")
		require.Error(t, err, "U+%04X must not become whitespace", r)
	}
}

func TestCommentsAreSingleTokens(t *testing.T) {
	for _, sql := range []string{"-- hidden", "// hidden", "# hidden", "#!hidden", "/* outer /* inner */ tail */"} {
		tokens, err := chsql.Tokenize(sql)
		require.NoError(t, err)
		require.Len(t, tokens, 1, sql)
		require.Equal(t, chsql.Comment, tokens[0].Kind, sql)
		require.Equal(t, sql, tokens[0].Text)
	}
}

func TestTruncatedUTF8FailsWithoutPanic(t *testing.T) {
	for _, whole := range []string{"‘x’", "“x”", "\u0085", "\u180e", "\u200b", "\u2060", "\ufeff", "−"} {
		for end := 1; end < len(whole); end++ {
			_, err := chsql.Tokenize(whole[:end])
			require.Error(t, err, "truncated token %x", whole[:end])
		}
	}
}

func TestQuotedTokenKinds(t *testing.T) {
	for _, tc := range []struct {
		text string
		kind chsql.TokenKind
	}{
		{"x'4142'", chsql.String},
		{"X'4142'", chsql.String},
		{"b'01'", chsql.String},
		{"B'01'", chsql.String},
		{"x''", chsql.String},
		{"b''", chsql.String},
		{"'a''b'", chsql.String},
		{"‘x’", chsql.String},
		{"\"x\"", chsql.QuotedIdent},
		{"`x`", chsql.QuotedIdent},
		{"“x”", chsql.QuotedIdent},
	} {
		tokens, err := chsql.Tokenize(tc.text)
		require.NoError(t, err)
		require.Len(t, tokens, 1, tc.text)
		require.Equal(t, tc.kind, tokens[0].Kind, tc.text)
		require.Equal(t, tc.text, tokens[0].Text)
	}
	for _, bad := range []string{"X'4G'", "B'012'", "X'", "B'"} {
		_, err := chsql.Tokenize(bad)
		require.Error(t, err, bad)
	}
}

func TestInvalidUTF8BytesFailClosed(t *testing.T) {
	for _, sql := range []string{"\x80", "\xff", "SELECT \x80", "SELECT \xc2", "SELECT \xe2\x80"} {
		_, err := chsql.Tokenize(sql)
		require.Error(t, err, "invalid unquoted bytes %x", sql)
	}
}

func TestNumericSeparatorPreservesFollowingComment(t *testing.T) {
	for _, prefix := range []string{"1_e", "0x1_p"} {
		tokens, err := chsql.Tokenize(prefix + "-- file(1)")
		require.NoError(t, err)
		require.Len(t, tokens, 2)
		require.Equal(t, chsql.Word, tokens[0].Kind)
		require.Equal(t, prefix, tokens[0].Text)
		require.Equal(t, chsql.Comment, tokens[1].Kind)
		require.Equal(t, "-- file(1)", tokens[1].Text)
	}
}
