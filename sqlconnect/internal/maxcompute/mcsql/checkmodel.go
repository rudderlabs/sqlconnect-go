package mcsql

import (
	"errors"
	"strings"
)

// ErrModelSQL is the error class of [CheckModelSQL]. The error text names the
// rule, never the SQL.
var ErrModelSQL = errors.New("model SQL refused")

// writeWords are refused anywhere in model SQL, outside strings, backtick
// identifiers, comments and qualified names (t.update). WITH … INSERT is a
// write, so a leading WITH is not enough.
var writeWords = map[string]bool{
	"INSERT": true, "DELETE": true, "UPDATE": true, "MERGE": true, "CREATE": true, "DROP": true,
	"ALTER": true, "TRUNCATE": true, "GRANT": true, "REVOKE": true, "INSTALL": true, "OVERWRITE": true,
}

// CheckModelSQL is the one implementation of the HLD settings rule: model SQL
// must be one read-only SELECT or WITH … SELECT statement, because a customer
// SET either fails or silently does nothing for later jobs, and several
// statements or a hint would change what the job does. It refuses:
//   - text it cannot scan (an unterminated string, identifier or comment);
//   - a top-level semicolon other than one trailing semicolon;
//   - a statement that does not start with SELECT or WITH, which covers SET,
//     ADD, CREATE, DROP, ALTER, INSERT, DELETE, UPDATE, MERGE, TRUNCATE,
//     GRANT, REVOKE, INSTALL, EXPLAIN, COST and FROM-first multi-inserts;
//   - a write keyword anywhere, such as WITH … INSERT;
//   - a script variable (@name);
//   - an inline hint comment (/*+ … */).
func CheckModelSQL(sql string) error {
	tokens, err := Scan(sql)
	if err != nil {
		return refuse("it cannot be scanned: " + err.Error())
	}
	var code []Token
	semicolons := 0
	for _, t := range tokens {
		switch {
		case t.Kind == Hint:
			return refuse("it holds a hint comment (/*+ … */)")
		case t.IsComment():
			continue
		case t.Kind == Semicolon:
			semicolons++
			continue
		case semicolons > 0:
			return refuse("it holds several statements")
		}
		code = append(code, t)
	}
	if semicolons > 1 {
		return refuse("it holds several statements")
	}
	if len(code) == 0 {
		return refuse("it is empty")
	}
	first := ""
	for _, t := range code {
		if t.Kind == Punct && t.Text == "(" {
			continue
		}
		if t.Kind == Word {
			first = strings.ToUpper(t.Text)
		}
		break
	}
	if first != "SELECT" && first != "WITH" {
		return refuse("it must start with SELECT or WITH")
	}
	for i, t := range code {
		if t.Kind == Punct && t.Text == "@" {
			return refuse("it holds a script variable (@)")
		}
		if t.Kind != Word || (i > 0 && code[i-1].Kind == Punct && code[i-1].Text == ".") {
			continue
		}
		if word := strings.ToUpper(t.Text); writeWords[word] {
			return refuse("it holds the write keyword " + word)
		}
	}
	return nil
}

func refuse(reason string) error {
	return &modelSQLError{reason: reason}
}

type modelSQLError struct{ reason string }

func (e *modelSQLError) Error() string { return "model SQL refused: " + e.reason }
func (e *modelSQLError) Unwrap() error { return ErrModelSQL }
