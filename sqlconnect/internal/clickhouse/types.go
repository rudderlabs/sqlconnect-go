package clickhouse

import (
	"slices"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

// chType is one parsed ClickHouse type declaration.
type chType struct {
	Name string
	// Args holds the trimmed text of each argument in the parentheses.
	Args []string
	// Children holds the parsed child types of Nullable, LowCardinality, Array,
	// Map, an unnamed Tuple, and the type argument of SimpleAggregateFunction.
	Children []chType
	// Fields holds the named fields of a Tuple or Nested.
	Fields []tupleField
	// Unsupported marks a Tuple or Nested that the clickhouse-go decoder cannot
	// read, and every Array, Nullable, LowCardinality, Tuple or Nested around it.
	Unsupported bool
}

type tupleField struct {
	Name string
	Type chType
}

// unwrap strips Nullable and LowCardinality.
func (t chType) unwrap() chType {
	for (t.Name == "Nullable" || t.Name == "LowCardinality") && len(t.Children) == 1 {
		t = t.Children[0]
	}
	return t
}

// nullable reports a Nullable wrapper, directly or inside LowCardinality.
func (t chType) nullable() bool {
	for len(t.Children) == 1 {
		switch t.Name {
		case "Nullable":
			return true
		case "LowCardinality":
			t = t.Children[0]
		default:
			return false
		}
	}
	return false
}

// canonicalType returns the canonical label of a declaration, or
// "unsupported" when the declaration does not parse.
func canonicalType(raw string) string {
	t, err := parseType(raw)
	if err != nil {
		return "unsupported"
	}
	return label(t)
}

// label is a pure function of the declaration text: no value, setting or
// connection changes it.
func label(t chType) string {
	t = t.unwrap()
	if t.Unsupported {
		return "unsupported"
	}
	switch t.Name {
	case "Bool":
		return "boolean"
	case "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32":
		return "int"
	case "UInt64", "Int128", "Int256", "UInt128", "UInt256", "Decimal", "Decimal32", "Decimal64",
		"Decimal128", "Decimal256", "String", "FixedString", "Time", "Time64", "Enum8", "Enum16",
		"UUID", "IPv4", "IPv6":
		return "string"
	case "Float32", "Float64":
		return "float"
	case "Date", "Date32", "DateTime", "DateTime64":
		return "datetime"
	case "JSON", "Object":
		return "json"
	case "SimpleAggregateFunction":
		return label(t.Children[0])
	case "Array", "Nested", "Tuple":
		for _, c := range append(slices.Clone(t.Children), fieldTypes(t.Fields)...) {
			if label(c) == "unsupported" {
				return "unsupported"
			}
		}
		if t.Name == "Tuple" {
			return "json"
		}
		return "array"
	}
	return "unsupported" // Map, AggregateFunction, Variant, Dynamic, unknown
}

func fieldTypes(fields []tupleField) []chType {
	out := make([]chType, 0, len(fields))
	for _, f := range fields {
		out = append(out, f.Type)
	}
	return out
}

// parseType parses exactly one type expression: a name, one optional
// argument list, then the end of input. RawType reaches CREATE TABLE
// verbatim (see renderCreate), so outside quoted text only a small safe
// character set is accepted and any trailing text is an error.
func parseType(raw string) (chType, error) {
	p := &typeParser{s: []rune(raw)}
	t, err := p.typeExpr()
	if err != nil {
		return chType{}, err
	}
	p.skipSpace()
	if !p.eof() {
		return chType{}, errTypeSyntax("text after the type")
	}
	return t, nil
}

func errTypeSyntax(detail string) error {
	return cherr.New(cherr.CodeQueryInvalid, "type", "the type declaration is malformed: "+detail)
}

type typeParser struct {
	s []rune
	i int
}

func (p *typeParser) eof() bool { return p.i >= len(p.s) }

func (p *typeParser) skipSpace() {
	for !p.eof() && isTypeSpace(p.s[p.i]) {
		p.i++
	}
}

func isTypeSpace(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' }

func isASCIILetter(r rune) bool { return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') }

func isASCIIDigit(r rune) bool { return r >= '0' && r <= '9' }

// isSafeUnquoted is the character set admitted outside quoted text. A '-' is
// admitted for negative enum values, never as a "--" comment.
func isSafeUnquoted(r rune) bool {
	if isASCIILetter(r) || isASCIIDigit(r) || isTypeSpace(r) {
		return true
	}
	switch r {
	case '_', '(', ')', ',', '=', '+', '-', '.':
		return true
	}
	return false
}

// argument is the text of one argument and whether a quoted part inside it
// holds a comma or a parenthesis.
type argument struct {
	text          string
	quotedSpecial bool
}

func (p *typeParser) typeExpr() (chType, error) {
	p.skipSpace()
	start := p.i
	if p.eof() || !isASCIILetter(p.s[p.i]) {
		return chType{}, errTypeSyntax("a type name must start with a letter")
	}
	for !p.eof() && (isASCIILetter(p.s[p.i]) || isASCIIDigit(p.s[p.i])) {
		p.i++
	}
	t := chType{Name: string(p.s[start:p.i])}
	save := p.i
	p.skipSpace()
	if p.eof() || p.s[p.i] != '(' {
		p.i = save
		return t, nil
	}
	p.i++
	args, err := p.argumentList()
	if err != nil {
		return chType{}, err
	}
	for _, a := range args {
		t.Args = append(t.Args, a.text)
	}
	if err := t.buildChildren(args); err != nil {
		return chType{}, err
	}
	return t, nil
}

// argumentList reads the arguments up to and including the matching ')'. It
// splits at commas of depth zero outside quoted text. Inside quoted text a
// backslash escapes the next character and a doubled quote is one quote.
func (p *typeParser) argumentList() ([]argument, error) {
	var (
		args    []argument
		depth   int
		start   = p.i
		special bool
	)
	finish := func() error {
		text := strings.TrimSpace(string(p.s[start:p.i]))
		if text == "" {
			return errTypeSyntax("an empty argument")
		}
		args = append(args, argument{text: text, quotedSpecial: special})
		special = false
		return nil
	}
	for {
		if p.eof() {
			return nil, errTypeSyntax("an unclosed parenthesis")
		}
		r := p.s[p.i]
		switch {
		case r == '\'' || r == '`' || r == '"':
			s, err := p.quoted()
			if err != nil {
				return nil, err
			}
			special = special || strings.ContainsAny(s, ",()")
			continue
		case r == '(':
			depth++
		case r == ')':
			if depth == 0 {
				if err := finish(); err != nil {
					return nil, err
				}
				p.i++
				return args, nil
			}
			depth--
		case r == ',' && depth == 0:
			if err := finish(); err != nil {
				return nil, err
			}
			p.i++
			start = p.i
			continue
		case r == '-' && p.i+1 < len(p.s) && p.s[p.i+1] == '-':
			return nil, errTypeSyntax("a comment")
		case !isSafeUnquoted(r):
			return nil, errTypeSyntax("a character outside quoted text that a type cannot hold")
		}
		p.i++
	}
}

// quoted reads one quoted part at p.i and returns its decoded text.
func (p *typeParser) quoted() (string, error) {
	q := p.s[p.i]
	p.i++
	var b strings.Builder
	for {
		if p.eof() {
			return "", errTypeSyntax("an unclosed quote")
		}
		r := p.s[p.i]
		switch {
		case r == 0:
			return "", errTypeSyntax("NUL in quoted text")
		case r == '\\':
			if p.i+1 >= len(p.s) {
				return "", errTypeSyntax("an unclosed quote")
			}
			b.WriteRune(p.s[p.i+1])
			p.i += 2
		case r == q && p.i+1 < len(p.s) && p.s[p.i+1] == q:
			b.WriteRune(q)
			p.i += 2
		case r == q:
			p.i++
			return b.String(), nil
		default:
			b.WriteRune(r)
			p.i++
		}
	}
}

func (t *chType) buildChildren(args []argument) error {
	want := func(n int) error {
		if len(args) != n {
			return errTypeSyntax(t.Name + " takes a different number of arguments")
		}
		return nil
	}
	child := func(text string) (chType, error) {
		c, err := parseType(text)
		if err != nil {
			return chType{}, err
		}
		t.Children = append(t.Children, c)
		return c, nil
	}
	switch t.Name {
	case "Nullable", "LowCardinality", "Array":
		if err := want(1); err != nil {
			return err
		}
		c, err := child(args[0].text)
		if err != nil {
			return err
		}
		t.Unsupported = c.Unsupported
	case "Map":
		if err := want(2); err != nil {
			return err
		}
		for _, a := range args {
			if _, err := child(a.text); err != nil {
				return err
			}
		}
	case "SimpleAggregateFunction":
		if err := want(2); err != nil {
			return err
		}
		if _, err := child(args[1].text); err != nil {
			return err
		}
	case "Tuple", "Nested":
		return t.buildFields(args)
	}
	return nil
}

// buildFields parses the elements of a Tuple or Nested. An element is a type
// or "<name> <type>", where the name is bare or one quoted identifier.
func (t *chType) buildFields(args []argument) error {
	seen := map[string]bool{}
	for _, a := range args {
		if a.quotedSpecial {
			t.Unsupported = true
		}
		p := &typeParser{s: []rune(a.text)}
		name, quoted, err := p.fieldName()
		if err != nil {
			return err
		}
		ty, err := p.typeExpr()
		if err != nil {
			return err
		}
		p.skipSpace()
		if !p.eof() {
			return errTypeSyntax("text after a " + t.Name + " element")
		}
		if quoted {
			t.Unsupported = true
		}
		if ty.Unsupported {
			t.Unsupported = true
		}
		if name == "" {
			t.Children = append(t.Children, ty)
			continue
		}
		if seen[name] {
			return cherr.New(cherr.CodeDuplicateColumn, "type", "a "+t.Name+" declares the same field name twice")
		}
		seen[name] = true
		t.Fields = append(t.Fields, tupleField{Name: name, Type: ty})
	}
	return nil
}

// fieldName reads an optional element name. A bare word followed by space and
// then something other than '(' is a name; otherwise the word starts the type.
func (p *typeParser) fieldName() (name string, quoted bool, err error) {
	p.skipSpace()
	if p.eof() {
		return "", false, errTypeSyntax("an empty element")
	}
	if r := p.s[p.i]; r == '`' || r == '"' {
		name, err = p.quoted()
		if err != nil {
			return "", false, err
		}
		if name == "" {
			return "", false, errTypeSyntax("an empty field name")
		}
		return name, true, nil
	}
	start := p.i
	if !isASCIILetter(p.s[p.i]) && p.s[p.i] != '_' {
		return "", false, errTypeSyntax("a field name or type must start with a letter")
	}
	for !p.eof() && (isASCIILetter(p.s[p.i]) || isASCIIDigit(p.s[p.i]) || p.s[p.i] == '_') {
		p.i++
	}
	word := string(p.s[start:p.i])
	afterWord := p.i
	p.skipSpace()
	if p.i == afterWord || p.eof() || p.s[p.i] == '(' {
		p.i = start
		return "", false, nil
	}
	return word, false, nil
}
