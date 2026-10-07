// Package chsql is the ClickHouse SQL lexer that the query guard and the SQL
// wrappers share. It follows the token rules of the ClickHouse 26.3 lexer
// (src/Parsers/Lexer.cpp) for every state that can hide a quote, a comment or a
// keyword: quoted strings and identifiers with backslash escapes and doubled
// quotes, heredocs, Unicode quotes, nested block comments, line comments, UTF-8
// whitespace and numbers with an exponent sign. Text that the server lexer
// turns into an error token fails Tokenize, so no caller scans past it.
package chsql

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

type TokenKind int

const (
	Word TokenKind = iota
	QuotedIdent
	String
	Number
	Punct
	Comment
	Space
)

type Token struct {
	Kind  TokenKind
	Text  string
	Upper string // Text in upper case, set for Word tokens only
	Depth int    // nesting depth of (), [] and {} at the token; an opening bracket counts itself
	Pos   int    // byte offset of Text in the input
}

// Tokenize splits s into tokens. The tokens cover s without gaps, so joining
// their Text gives s back. It fails on an unclosed quote or block comment, or on
// any byte sequence the ClickHouse lexer rejects.
func Tokenize(s string) ([]Token, error) {
	l := lexer{s: s}
	for l.i < len(s) {
		if err := l.next(); err != nil {
			return nil, err
		}
	}
	return l.out, nil
}

type lexer struct {
	s     string
	i     int
	depth int
	out   []Token
	// prev is the index in out of the last token that is not Space or
	// Comment; hasPrev reports whether one exists. The ClickHouse lexer uses
	// it to tell a tuple-access dot from the start of a number.
	prev    int
	hasPrev bool
}

func (l *lexer) emit(k TokenKind, start int) {
	t := Token{Kind: k, Text: l.s[start:l.i], Depth: l.depth, Pos: start}
	if k == Word {
		t.Upper = strings.ToUpper(t.Text)
	}
	l.out = append(l.out, t)
	if k != Space && k != Comment {
		l.prev, l.hasPrev = len(l.out)-1, true
	}
}

func (l *lexer) at(j int) byte {
	if j < len(l.s) {
		return l.s[j]
	}
	return 0
}

func (l *lexer) next() error {
	s, start := l.s, l.i
	c := s[l.i]
	switch {
	case isSpaceASCII(c) || utf8SpaceLen(s, l.i) > 0:
		l.skipSpace()
		l.emit(Space, start)
	case c >= '0' && c <= '9':
		return l.number(start)
	case c == '\'':
		return l.quoted(start, '\'', String)
	case c == '"':
		return l.quoted(start, '"', QuotedIdent)
	case c == '`':
		return l.quoted(start, '`', QuotedIdent)
	case c == '(' || c == '[' || c == '{':
		l.depth++
		l.i++
		l.emit(Punct, start)
	case c == ')' || c == ']' || c == '}':
		l.i++
		l.emit(Punct, start)
		l.depth--
	case c == '.':
		return l.dot(start)
	case c == '-' && l.at(l.i+1) == '-', c == '/' && l.at(l.i+1) == '/':
		l.lineComment()
		l.emit(Comment, start)
	case c == '#':
		if n := l.at(l.i + 1); n != ' ' && n != '!' {
			return fmt.Errorf("unexpected '#' at %d", start)
		}
		l.lineComment()
		l.emit(Comment, start)
	case c == '/' && l.at(l.i+1) == '*':
		if err := l.blockComment(start); err != nil {
			return err
		}
		l.emit(Comment, start)
	case c == '!':
		if l.at(l.i+1) != '=' {
			return fmt.Errorf("unexpected '!' at %d", start)
		}
		l.i += 2
		l.emit(Punct, start)
	case c == '\\':
		// ClickHouse reads "\G" as a vertical-format delimiter and anything
		// else as an error. Neither belongs in a scanned query.
		return fmt.Errorf("unexpected backslash at %d", start)
	case c == 0xE2 && l.at(l.i+1) == 0x88 && l.at(l.i+2) == 0x92:
		l.i += 3 // U+2212 MINUS SIGN
		l.emit(Punct, start)
	case c == 0xE2 && l.i+5 < len(s) && s[l.i+1] == 0x80 && (s[l.i+2] == 0x98 || s[l.i+2] == 0x9C):
		return l.unicodeQuoted(start)
	case c == '$':
		return l.dollar(start)
	case (c == 'x' || c == 'X' || c == 'b' || c == 'B') && l.i+2 < len(s) && s[l.i+1] == '\'':
		return l.hexOrBinString(start)
	case isWordChar(c):
		l.bareWord()
		l.emit(Word, start)
	case c >= 0x80:
		return fmt.Errorf("unexpected non-ASCII byte outside a quoted string at %d", start)
	default:
		l.i++
		l.emit(Punct, start)
	}
	return nil
}

func (l *lexer) skipSpace() {
	for l.i < len(l.s) {
		if isSpaceASCII(l.s[l.i]) {
			l.i++
		} else if n := utf8SpaceLen(l.s, l.i); n > 0 {
			l.i += n
		} else {
			return
		}
	}
}

func (l *lexer) lineComment() {
	if j := strings.IndexByte(l.s[l.i:], '\n'); j >= 0 {
		l.i += j
	} else {
		l.i = len(l.s)
	}
}

// blockComment consumes a /* */ comment. ClickHouse nests them.
func (l *lexer) blockComment(start int) error {
	l.i += 2
	for level := 1; l.i+2 <= len(l.s); {
		switch {
		case l.s[l.i] == '/' && l.s[l.i+1] == '*':
			level++
			l.i += 2
		case l.s[l.i] == '*' && l.s[l.i+1] == '/':
			level--
			l.i += 2
			if level == 0 {
				return nil
			}
		default:
			l.i++
		}
	}
	return fmt.Errorf("unclosed block comment at %d", start)
}

// quoted consumes a string or identifier with backslash escapes and doubled
// quote characters.
func (l *lexer) quoted(start int, q byte, k TokenKind) error {
	for j := l.i + 1; j < len(l.s); j++ {
		switch l.s[j] {
		case '\\':
			j++
		case q:
			if j+1 < len(l.s) && l.s[j+1] == q {
				j++
				continue
			}
			l.i = j + 1
			l.emit(k, start)
			return nil
		}
	}
	return fmt.Errorf("unclosed quote at %d", start)
}

// unicodeQuoted consumes ‘string’ or “identifier”. It has no escapes.
func (l *lexer) unicodeQuoted(start int) error {
	closing, kind := byte(0x99), String
	if l.s[l.i+2] == 0x9C {
		closing, kind = 0x9D, QuotedIdent
	}
	for j := l.i + 3; ; j++ {
		k := strings.IndexByte(l.s[j:], 0xE2)
		if k < 0 || j+k+2 >= len(l.s) {
			return fmt.Errorf("unclosed quote at %d", start)
		}
		j += k
		if l.s[j+1] == 0x80 && l.s[j+2] == closing {
			l.i = j + 3
			l.emit(kind, start)
			return nil
		}
	}
}

// dollar handles a heredoc ($tag$ ... $tag$, the tag made of word characters),
// a standalone dollar sign, or a bare word that starts with '$'.
func (l *lexer) dollar(start int) error {
	s := l.s
	if k := strings.IndexByte(s[l.i+1:], '$'); k >= 0 {
		tagEnd := l.i + 1 + k
		valid := true
		for j := l.i + 1; j < tagEnd; j++ {
			if !isWordChar(s[j]) {
				valid = false
				break
			}
		}
		if valid {
			tag := s[l.i : tagEnd+1]
			if e := strings.Index(s[tagEnd+1:], tag); e >= 0 {
				l.i = tagEnd + 1 + e + len(tag)
				l.emit(String, start)
				return nil
			}
		}
	}
	if !isWordChar(l.at(l.i + 1)) {
		l.i++
		l.emit(Punct, start)
		return nil
	}
	l.bareWord()
	l.emit(Word, start)
	return nil
}

func (l *lexer) hexOrBinString(start int) error {
	hex := l.s[l.i] == 'x' || l.s[l.i] == 'X'
	l.i += 2
	for l.i < len(l.s) && (hex && isHexDigit(l.s[l.i]) || !hex && (l.s[l.i] == '0' || l.s[l.i] == '1')) {
		l.i++
	}
	if l.i >= len(l.s) || l.s[l.i] != '\'' {
		return fmt.Errorf("malformed hex or binary string at %d", start)
	}
	l.i++
	l.emit(String, start)
	return nil
}

func (l *lexer) bareWord() {
	l.i++
	for l.i < len(l.s) && (isWordChar(l.s[l.i]) || l.s[l.i] == '$') {
		l.i++
	}
}

// dot emits a tuple-access or qualifier dot, or scans a number that starts
// with '.', by the rule of the ClickHouse lexer.
func (l *lexer) dot(start int) error {
	if start > 0 && (!isDigit(l.at(l.i+1)) || l.prevAllowsDot()) {
		l.i++
		l.emit(Punct, start)
		return nil
	}
	l.i++
	l.digits(true, false)
	l.exponent(false)
	l.emit(Number, start)
	return nil
}

func (l *lexer) prevAllowsDot() bool {
	if !l.hasPrev {
		return false
	}
	p := l.out[l.prev]
	switch p.Kind {
	case Word, QuotedIdent, Number:
		return true
	case Punct:
		return p.Text == ")" || p.Text == "]"
	}
	return false
}

func (l *lexer) number(start int) error {
	s := l.s
	afterDot := l.hasPrev && l.out[l.prev].Kind == Punct && l.out[l.prev].Text == "."
	if afterDot {
		// Tuple access such as x.1.1 reads a plain integer.
		l.i++
		l.digits(false, false)
	} else {
		hex, block := false, false
		if l.i+2 < len(s) && s[l.i] == '0' && strings.IndexByte("xXbB", s[l.i+1]) >= 0 {
			var valid bool
			if s[l.i+1] == 'x' || s[l.i+1] == 'X' {
				hex = isHexDigit(s[l.i+2])
				valid = hex
			} else {
				valid = s[l.i+2] == '0' || s[l.i+2] == '1'
			}
			if valid {
				l.i += 2
				block = true
			} else {
				l.i++
			}
		} else {
			l.i++
		}
		l.digits(block, hex)
		if l.at(l.i) == '.' {
			l.i++
			l.digits(true, hex)
		}
		l.exponent(hex)
	}
	if l.i < len(s) && isWordChar(s[l.i]) {
		for l.i < len(s) && isWordChar(s[l.i]) {
			l.i++
		}
		for j := start; j < l.i; j++ {
			if !isWordChar(s[j]) && s[j] != '$' {
				return fmt.Errorf("malformed number at %d", start)
			}
		}
		l.emit(Word, start)
		return nil
	}
	l.emit(Number, start)
	return nil
}

func (l *lexer) digits(block, hex bool) {
	for l.i < len(l.s) {
		c := l.s[l.i]
		if hex && isHexDigit(c) || !hex && isDigit(c) || l.isNumberSeparator(block, hex) {
			l.i++
			block = false
			continue
		}
		return
	}
}

func (l *lexer) exponent(hex bool) {
	c := l.at(l.i)
	isExp := hex && (c == 'p' || c == 'P') || !hex && (c == 'e' || c == 'E')
	if l.i+1 >= len(l.s) || !isExp {
		return
	}
	l.i++
	if l.i+1 < len(l.s) && (l.s[l.i] == '-' || l.s[l.i] == '+') {
		l.i++
	}
	l.digits(true, false)
}

// isNumberSeparator mirrors ClickHouse: an underscore between two digits.
func (l *lexer) isNumberSeparator(block, hex bool) bool {
	if l.s[l.i] != '_' || block || l.i+1 >= len(l.s) {
		return false
	}
	n := l.s[l.i+1]
	return hex && isHexDigit(n) || !hex && isDigit(n)
}

// TrimTerminalSemicolon strips one terminal ';' and the whitespace around it,
// outside quotes and comments. It refuses a second statement and any text,
// a comment included, after the semicolon.
func TrimTerminalSemicolon(s string) (string, error) {
	toks, err := Tokenize(s)
	if err != nil {
		return "", err
	}
	last := -1
	for i, tok := range slices.Backward(toks) {
		if tok.Kind != Space {
			last = i
			break
		}
	}
	for i, tk := range toks {
		if tk.Kind == Punct && tk.Text == ";" && i != last {
			return "", errors.New("more than one statement")
		}
	}
	if last >= 0 && toks[last].Kind == Punct && toks[last].Text == ";" {
		last--
		for last >= 0 && toks[last].Kind == Space {
			last--
		}
	}
	if last < 0 {
		return "", nil
	}
	// Cut at the end of the last token that is not Space, so the trim uses
	// the lexer's whitespace set, U+FEFF and U+200B included.
	return s[:toks[last].Pos+len(toks[last].Text)], nil
}

func isDigit(c byte) bool    { return c >= '0' && c <= '9' }
func isHexDigit(c byte) bool { return isDigit(c) || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }
func isWordChar(c byte) bool {
	return c == '_' || isDigit(c) || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z'
}

func isSpaceASCII(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

// utf8SpaceLen returns the byte length of the UTF-8 whitespace that ClickHouse
// skips at s[i] (skipWhitespacesUTF8 in src/Common/StringUtils.h), or 0. The
// bounds checks match the server's, including its need for one byte after a
// three-byte sequence.
func utf8SpaceLen(s string, i int) int {
	if i+1 < len(s) && s[i] == 0xC2 && (s[i+1] == 0x85 || s[i+1] == 0xA0) {
		return 2
	}
	if i+2 >= len(s) {
		return 0
	}
	a, b, c := s[i], s[i+1], s[i+2]
	switch {
	case a == 0xE1 && b == 0xA0 && c == 0x8E,
		a == 0xE2 && b == 0x80 && (c >= 0x80 && c <= 0x8D || c == 0xA8 || c == 0xA9 || c == 0xAF),
		a == 0xE2 && b == 0x81 && (c == 0x9F || c == 0xA0),
		a == 0xE3 && b == 0x80 && c == 0x80,
		a == 0xEF && b == 0xBB && c == 0xBF:
		return 3
	}
	return 0
}
