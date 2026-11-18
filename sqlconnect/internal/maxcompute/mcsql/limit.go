package mcsql

import "strings"

// LimitZero returns sql unchanged when it has a top-level LIMIT, and
// otherwise wraps it as SELECT * FROM (<sql>) sq LIMIT 0, so that reading a
// query's columns does not run it in full. A trailing semicolon is dropped.
func LimitZero(sql string) (string, error) {
	tokens, err := Scan(sql)
	if err != nil {
		return "", err
	}
	depth := 0
	end := len(sql)
	for _, t := range tokens {
		switch {
		case t.Kind == Punct && t.Text == "(":
			depth++
		case t.Kind == Punct && t.Text == ")":
			depth--
		case t.Kind == Word && depth == 0 && strings.EqualFold(t.Text, "LIMIT"):
			return sql, nil
		case t.Kind == Semicolon && depth == 0:
			end = min(end, t.Start)
		}
	}
	return "SELECT * FROM (\n" + strings.TrimSpace(sql[:end]) + "\n) sq LIMIT 0", nil
}
