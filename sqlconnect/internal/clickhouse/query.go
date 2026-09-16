package clickhouse

import "strings"

// selectQuery checks the statement boundary while leaving SELECT grammar to the
// server. Quoted semicolons and comments are not statement delimiters.
func selectQuery(query string) (string, error) {
	invalid := func() (string, error) {
		return "", adapterError("CH_INVALID_QUERY", "expected one SELECT or WITH query without FORMAT", nil)
	}
	query = strings.TrimSpace(query)
	if query == "" || strings.ContainsRune(query, 0) {
		return invalid()
	}
	var quote byte
	depth := 0
	first := ""
	ended := false
	end := len(query)
	for i := 0; i < len(query); i++ {
		c := query[i]
		if quote != 0 {
			if c == '\\' {
				i++
				if i >= len(query) {
					return invalid()
				}
				continue
			}
			if c == quote {
				if i+1 < len(query) && query[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		if c == ' ' || c == '\n' || c == '\r' || c == '\t' {
			continue
		}
		if c == '-' && i+1 < len(query) && query[i+1] == '-' {
			i += 2
			for i < len(query) && query[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(query) && query[i+1] == '*' {
			j := strings.Index(query[i+2:], "*/")
			if j < 0 {
				return invalid()
			}
			i += j + 3
			continue
		}
		if ended {
			return invalid()
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return invalid()
			}
		case ';':
			if depth != 0 {
				return invalid()
			}
			ended = true
			end = i
		default:
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
				j := i + 1
				for j < len(query) && ((query[j] >= 'a' && query[j] <= 'z') || (query[j] >= 'A' && query[j] <= 'Z') || (query[j] >= '0' && query[j] <= '9') || query[j] == '_') {
					j++
				}
				word := strings.ToUpper(query[i:j])
				if first == "" {
					first = word
				}
				if word == "FORMAT" && depth == 0 {
					return invalid()
				}
				i = j - 1
			}
		}
	}
	if quote != 0 || depth != 0 || (first != "SELECT" && first != "WITH") {
		return invalid()
	}
	// A newline keeps a trailing line comment from swallowing the wrapper's ')'.
	return strings.TrimSpace(query[:end]) + "\n", nil
}
