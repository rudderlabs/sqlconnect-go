package clickhouse

import (
	"strconv"
	"strings"
	"unicode"
)

type nativeType struct {
	name string
	args []string
}

func parseType(raw string) (nativeType, bool) {
	raw = strings.TrimSpace(raw)
	name, body, hasArgs := strings.Cut(raw, "(")
	name = strings.TrimSpace(name)
	if name == "" {
		return nativeType{}, false
	}
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return nativeType{}, false
		}
	}
	result := nativeType{name: name}
	if !hasArgs {
		return result, true
	}
	if !strings.HasSuffix(body, ")") {
		return nativeType{}, false
	}
	body = body[:len(body)-1]
	depth, start := 0, 0
	var quote byte
	for i := 0; i < len(body); i++ {
		c := body[i]
		if quote != 0 {
			if c == '\\' {
				i++
				if i >= len(body) {
					return nativeType{}, false
				}
				continue
			}
			if c == quote {
				if i+1 < len(body) && body[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nativeType{}, false
			}
		case ',':
			if depth == 0 {
				result.args = append(result.args, strings.TrimSpace(body[start:i]))
				start = i + 1
			}
		case ';':
			return nativeType{}, false
		}
	}
	if depth != 0 || quote != 0 {
		return nativeType{}, false
	}
	result.args = append(result.args, strings.TrimSpace(body[start:]))
	for _, arg := range result.args {
		if arg == "" {
			return nativeType{}, false
		}
	}
	return result, true
}

func tupleFields(args []string) (names, types []string, ok bool) {
	seen := map[string]bool{}
	named := false
	for i, arg := range args {
		if strings.HasPrefix(arg, "`") || strings.HasPrefix(arg, `"`) {
			return nil, nil, false
		}
		// The native decoder cannot safely split quoted tuple labels with delimiters.
		var quote byte
		for j := 0; j < len(arg); j++ {
			c := arg[j]
			if quote != 0 {
				if c == ',' || c == '(' || c == ')' {
					return nil, nil, false
				}
				if c == '\\' {
					j++
					continue
				}
				if c == quote {
					quote = 0
				}
			} else if c == '\'' || c == '"' || c == '`' {
				quote = c
			}
		}
		split := strings.IndexFunc(arg, unicode.IsSpace)
		paren := strings.IndexByte(arg, '(')
		name, raw := "", arg
		if split >= 0 && (paren < 0 || split < paren) {
			name, raw = arg[:split], strings.TrimSpace(arg[split:])
			for j, r := range name {
				if !(unicode.IsLetter(r) || r == '_' || (j > 0 && unicode.IsDigit(r))) {
					return nil, nil, false
				}
			}
			if seen[name] {
				return nil, nil, false
			}
			seen[name] = true
		}
		if i == 0 {
			named = name != ""
		} else if named != (name != "") {
			return nil, nil, false
		}
		names = append(names, name)
		types = append(types, raw)
	}
	return names, types, len(types) > 0
}

func canonicalType(raw string) string {
	typ, ok := parseType(raw)
	if !ok {
		return "unsupported"
	}
	switch typ.name {
	case "Nullable", "LowCardinality":
		if len(typ.args) == 1 {
			return canonicalType(typ.args[0])
		}
	case "SimpleAggregateFunction":
		if len(typ.args) == 2 {
			return canonicalType(typ.args[1])
		}
	case "Array":
		if len(typ.args) == 1 && canonicalType(typ.args[0]) != "unsupported" {
			return "array"
		}
	case "Tuple", "Nested":
		_, types, ok := tupleFields(typ.args)
		if !ok {
			return "unsupported"
		}
		for _, child := range types {
			if canonicalType(child) == "unsupported" {
				return "unsupported"
			}
		}
		if typ.name == "Nested" {
			return "array"
		}
		return "json"
	case "Bool":
		if len(typ.args) == 0 {
			return "boolean"
		}
	case "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32":
		if len(typ.args) == 0 {
			return "int"
		}
	case "Float32", "Float64":
		if len(typ.args) == 0 {
			return "float"
		}
	case "UInt64", "Int128", "Int256", "UInt128", "UInt256", "String", "UUID", "IPv4", "IPv6", "Time":
		if len(typ.args) == 0 {
			return "string"
		}
	case "Date", "Date32":
		if len(typ.args) == 0 {
			return "datetime"
		}
	case "DateTime":
		if len(typ.args) == 0 || (len(typ.args) == 1 && quotedString(typ.args[0])) {
			return "datetime"
		}
	case "DateTime64":
		if (len(typ.args) == 1 || (len(typ.args) == 2 && quotedString(typ.args[1]))) && numericArgs(typ.args[:1]) {
			return "datetime"
		}
	case "Time64", "FixedString":
		if len(typ.args) == 1 && numericArgs(typ.args) {
			return "string"
		}
	case "Decimal":
		if len(typ.args) == 2 && numericArgs(typ.args) {
			return "string"
		}
	case "Decimal32", "Decimal64", "Decimal128", "Decimal256":
		if len(typ.args) == 1 && numericArgs(typ.args) {
			return "string"
		}
	case "Enum8", "Enum16":
		if len(typ.args) == 0 {
			return "unsupported"
		}
		for _, arg := range typ.args {
			i := strings.LastIndex(arg, "=")
			if i < 0 || !quotedString(strings.TrimSpace(arg[:i])) {
				return "unsupported"
			}
			if _, err := strconv.ParseInt(strings.TrimSpace(arg[i+1:]), 10, 16); err != nil {
				return "unsupported"
			}
		}
		return "string"
	case "JSON":
		return "json"
	}
	return "unsupported"
}
func numericArgs(args []string) bool {
	for _, arg := range args {
		if _, err := strconv.ParseUint(arg, 10, 16); err != nil {
			return false
		}
	}
	return true
}
func quotedString(s string) bool { return len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' }
