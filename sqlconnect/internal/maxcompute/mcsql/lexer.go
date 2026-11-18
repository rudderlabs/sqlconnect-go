// Package mcsql scans MaxCompute SQL text for the boundaries the driver needs:
// strings, backtick identifiers, comments, statement separators and bind
// placeholders. It is not a parser.
package mcsql

import (
	"errors"
	"strings"
)

// Kind is the kind of a [Token].
type Kind int

const (
	// Word is a keyword or an unquoted identifier.
	Word Kind = iota + 1
	// String is a quoted string literal, '…' or "…", with backslash escapes.
	String
	// Quoted is a backtick-quoted identifier. MaxCompute has no escape inside it.
	Quoted
	// LineComment is a -- comment up to the end of the line.
	LineComment
	// BlockComment is a /* … */ comment.
	BlockComment
	// Hint is a /*+ … */ hint comment.
	Hint
	// Semicolon is a statement separator.
	Semicolon
	// Placeholder is a ? bind placeholder.
	Placeholder
	// Punct is any other character, such as an operator or @.
	Punct
)

// Token is a span of the scanned text.
type Token struct {
	Kind       Kind
	Start, End int // byte offsets: Text[Start:End]
	Text       string
}

// IsComment reports whether the token is a comment or a hint.
func (t Token) IsComment() bool {
	return t.Kind == LineComment || t.Kind == BlockComment || t.Kind == Hint
}

var (
	// ErrUnterminatedString is returned for a string literal without its closing quote.
	ErrUnterminatedString = errors.New("unterminated string literal")
	// ErrUnterminatedIdentifier is returned for a backtick identifier without its closing backtick.
	ErrUnterminatedIdentifier = errors.New("unterminated backtick identifier")
	// ErrUnterminatedComment is returned for a /* comment without its closing */.
	ErrUnterminatedComment = errors.New("unterminated comment")
)

// Scan splits sql into tokens. Whitespace is dropped.
func Scan(sql string) ([]Token, error) {
	var tokens []Token
	emit := func(kind Kind, start, end int) {
		tokens = append(tokens, Token{Kind: kind, Start: start, End: end, Text: sql[start:end]})
	}
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '\'' || c == '"':
			end, ok := scanString(sql, i)
			if !ok {
				return nil, ErrUnterminatedString
			}
			emit(String, i, end)
			i = end
		case c == '`':
			end := strings.IndexByte(sql[i+1:], '`')
			if end < 0 {
				return nil, ErrUnterminatedIdentifier
			}
			emit(Quoted, i, i+1+end+1)
			i = i + 1 + end + 1
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			end := strings.IndexByte(sql[i:], '\n')
			if end < 0 {
				end = len(sql)
			} else {
				end += i
			}
			emit(LineComment, i, end)
			i = end
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return nil, ErrUnterminatedComment
			}
			kind := BlockComment
			if i+2 < len(sql) && sql[i+2] == '+' {
				kind = Hint
			}
			emit(kind, i, i+2+end+2)
			i = i + 2 + end + 2
		case c == ';':
			emit(Semicolon, i, i+1)
			i++
		case c == '?':
			emit(Placeholder, i, i+1)
			i++
		case isWordByte(c):
			start := i
			for i < len(sql) && isWordByte(sql[i]) {
				i++
			}
			emit(Word, start, i)
		default:
			// One character at a time; a multi-byte rune becomes several Punct
			// tokens, which no caller inspects.
			emit(Punct, i, i+1)
			i++
		}
	}
	return tokens, nil
}

// scanString returns the offset after the string literal that starts at i.
// A backslash escapes the next byte: MaxCompute reads 'it”s' as two adjacent
// literals, not as an escaped quote.
func scanString(sql string, i int) (int, bool) {
	quote := sql[i]
	for j := i + 1; j < len(sql); j++ {
		switch sql[j] {
		case '\\':
			j++
		case quote:
			return j + 1, true
		}
	}
	return 0, false
}

func isWordByte(c byte) bool {
	return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c >= 0x80
}

// Statements splits tokens at top-level semicolons. Each statement holds its
// tokens without the separator. A statement of comments only, such as the text
// after a trailing semicolon, is dropped.
func Statements(tokens []Token) [][]Token {
	var out [][]Token
	var cur []Token
	flush := func() {
		for _, t := range cur {
			if !t.IsComment() {
				out = append(out, cur)
				break
			}
		}
		cur = nil
	}
	for _, t := range tokens {
		if t.Kind == Semicolon {
			flush()
			continue
		}
		cur = append(cur, t)
	}
	flush()
	return out
}

// Terminate returns sql with exactly the statement terminator the server
// needs: it appends ";" unless the text already ends in one, and puts it on a
// new line when the text ends in a line comment, which would swallow it.
func Terminate(sql string, tokens []Token) string {
	sql = strings.TrimRight(sql, " \t\n\r\f\v")
	for i := len(tokens) - 1; i >= 0; i-- {
		t := tokens[i]
		if t.Kind == Semicolon {
			return sql
		}
		if !t.IsComment() {
			break
		}
	}
	if n := len(tokens); n > 0 && tokens[n-1].Kind == LineComment {
		return sql + "\n;"
	}
	return sql + ";"
}
