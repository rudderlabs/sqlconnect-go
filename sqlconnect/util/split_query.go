package util

import (
	"strings"

	"github.com/samber/lo"
)

// SplitStatements splits a string containing multiple sql statements separated by semicolons.
// It strips out comments from the statements, both simple (--) and bracketed (/* */) ones.
// It also handles sql strings properly which can contain semi colons, escaped quotes and comment character sequences without affecting the splitting behaviour.
func SplitStatements(statements string) []string {
	var inString bool           // flag signalling that we are inside a SQL string
	var inEscapedQuote bool     // flag signalling that we are inside an escaped quote character inside a SQL string
	var inSimpleComment bool    // flag signalling that we are inside a simple comment (--)
	var inBracketedComment bool // flag signalling that we are inside a bracketed comment (/* */)

	var stmts []string //
	var stmt string
	var previous rune

	next := func(input string, i int) (rune, bool) {
		runes := []rune(input)
		if len(input) > i+1 {
			return runes[i+1], true
		}
		return 0, false
	}

	for i, c := range statements {
		if inString {
			if c == '\'' {
				if inEscapedQuote {
					inEscapedQuote = false
				} else {
					if next, ok := next(statements, i); ok {
						if next == '\'' {
							inEscapedQuote = true
						} else {
							inString = false
						}
					}
				}
			}
			stmt += string(c)
		} else if inSimpleComment {
			if c == '\n' {
				inSimpleComment = false
			}
		} else if inBracketedComment {
			if c == '/' && previous == '*' {
				inBracketedComment = false
			}
		} else {
			if c == '\'' {
				inString = true
				stmt += string(c)
			} else if c == '-' && previous == '-' {
				inSimpleComment = true
				stmtRunes := []rune(stmt)
				stmt = string(stmtRunes[:len(stmtRunes)-1]) // remove the previous dash
			} else if c == '*' && previous == '/' {
				inBracketedComment = true
				stmtRunes := []rune(stmt)
				stmt = string(stmtRunes[:len(stmtRunes)-1]) // remove the previous slash
			} else if c == ';' {
				stmts = append(stmts, stmt)
				stmt = ""
				continue
			} else {
				stmt += string(c)
			}
		}
		previous = c
	}
	if stmt != "" {
		stmts = append(stmts, stmt)
	}

	return lo.FilterMap(stmts, func(stmt string, _ int) (string, bool) {
		// remove leading and trailing whitespaces tabs and newlines
		stmt = strings.TrimRight(strings.TrimLeft(stmt, "\n\t "), "\n\t ")
		return stmt, stmt != ""
	})
}
