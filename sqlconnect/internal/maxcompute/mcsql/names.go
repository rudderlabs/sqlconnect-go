package mcsql

import (
	"errors"
	"strings"
)

// RefusedIdentifier is what [QuoteIdentifier] returns for a name it cannot
// quote. It holds NUL bytes, and the driver refuses any statement that holds
// one, so a refused name fails the statement locally instead of reaching the
// server. The dialect interface returns strings, not errors, so this is how a
// quoting refusal surfaces.
const RefusedIdentifier = "`\x00refused identifier\x00`"

// ErrInvalidName is returned for a project, schema or table name that holds a
// backtick, a dot or a NUL byte, or is empty.
var ErrInvalidName = errors.New("invalid name: it must not be empty or hold a backtick, a dot or a NUL byte")

// QuoteIdentifier quotes name with backticks. MaxCompute has no escape inside
// backticks (a doubled backtick is not one), so a name holding a backtick or a
// NUL byte is refused with [RefusedIdentifier].
func QuoteIdentifier(name string) string {
	if strings.ContainsAny(name, "`\x00") {
		return RefusedIdentifier
	}
	return "`" + name + "`"
}

// CheckName validates one part of a relation name: MaxCompute refuses a
// backtick or a dot in a project, schema or table name.
func CheckName(name string) error {
	if name == "" || strings.ContainsAny(name, "`.\x00") {
		return ErrInvalidName
	}
	return nil
}

// QuoteRelation quotes the non-empty parts and joins them with dots. A part
// that fails [CheckName] makes the whole name [RefusedIdentifier].
func QuoteRelation(parts ...string) string {
	var quoted []string
	for _, p := range parts {
		if p == "" {
			continue
		}
		if CheckName(p) != nil {
			return RefusedIdentifier
		}
		quoted = append(quoted, "`"+p+"`")
	}
	if len(quoted) == 0 {
		return RefusedIdentifier
	}
	return strings.Join(quoted, ".")
}

// Normalise folds a name the way MaxCompute does: every name is lower-cased,
// even inside backticks.
func Normalise(name string) string { return strings.ToLower(name) }
