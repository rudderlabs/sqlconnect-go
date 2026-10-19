package clickhouse

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNoSystemTableReads fails when a string literal in non-test code of the
// module names system.processes or system.query_log, or sends KILL QUERY. The
// driver must work the same whether or not a customer granted those tables
// (ACT2-1269). The query guard names them to refuse them, so it is exempt, and
// so is chtest, whose admin helpers read them as the server administrator.
func TestNoSystemTableReads(t *testing.T) {
	root, err := filepath.Abs("../../..")
	require.NoError(t, err)
	exempt := map[string]bool{
		filepath.Join(root, "sqlconnect", "clickhousequery", "queryguard.go"): true,
	}
	var hits []string
	scanned := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".worktrees", "testdata", "chtest":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || exempt[path] {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		scanned++
		for _, literal := range forbiddenSQLLiterals(f) {
			rel, _ := filepath.Rel(root, path)
			hits = append(hits, rel+": "+literal)
		}
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, scanned, 50, "the walk reached the module's source")
	require.Empty(t, hits, "non-test code must not read system.processes or system.query_log, nor send KILL QUERY")
}

var (
	systemIdentifier     = regexp.MustCompile(`\bsystem\b`)
	queryTableIdentifier = regexp.MustCompile(`\b(query_log|processes)\b`)
	killQueryStatement   = regexp.MustCompile(`(?i)\bKILL\s+QUERY\b`)
)

func forbiddenControlSQL(text string) bool {
	text = strings.ToLower(strings.Join(strings.Fields(text), " "))
	return systemIdentifier.MatchString(text) && queryTableIdentifier.MatchString(text) ||
		killQueryStatement.MatchString(text) || strings.Contains(text, "processlist")
}

func TestNoSystemTableReads_LiteralPatterns(t *testing.T) {
	for _, c := range []struct {
		sql       string
		forbidden bool
	}{
		{"SELECT * FROM system.processes", true},
		{"SELECT * FROM SYSTEM.QUERY_LOG", true},
		{"SELECT * FROM `system` . `processes`", true},
		{"SELECT * FROM system . query_log", true},
		{"system, processes", true},
		{"processes, system", true},
		{"query_log system", true},
		{"system\n\tprocesses", true},
		{"kIlL\n\tQUERY WHERE query_id = ?", true},
		{"KILL   QUERY", true},
		{"query_log", false},
		{"processes", false},
		{"SELECT * FROM system.tables", false},
		{"SELECT * FROM customer.processes", false},
		{"systematic processes", false},
		{"system processes_archive", false},
		{"system query_log_archive", false},
		{"KILL_QUERY", false},
	} {
		t.Run(c.sql, func(t *testing.T) {
			require.Equal(t, c.forbidden, forbiddenControlSQL(c.sql))
		})
	}
}

func forbiddenSQLLiterals(n ast.Node) []string {
	var hits []string
	ast.Inspect(n, func(n ast.Node) bool {
		expr, ok := n.(ast.Expr)
		if !ok {
			return true
		}
		text, ok := constantSQLString(expr)
		if !ok {
			return true
		}
		if forbiddenControlSQL(text) {
			hits = append(hits, text)
		}
		return false // the whole constant expression was checked

	})
	return hits
}

func TestNoSystemTableReads_ConstantStrings(t *testing.T) {
	for _, c := range []struct {
		source    string
		forbidden bool
	}{
		{`"KILL\n\tQUERY"`, true},
		{`"system\nprocesses"`, true},
		{`"system." + "processes"`, true},
		{`"query_log" + (" . " + "system")`, true},
		{`"KILL" + "\t" + "QUERY"`, true},
		{`"customer." + "processes"`, false},
		{`"system." + "processes_archive"`, false},
		{`"system." + "tables"`, false},
		{`f("system", "processes")`, false},
	} {
		t.Run(c.source, func(t *testing.T) {
			expr, err := parser.ParseExpr(c.source)
			require.NoError(t, err)
			require.Equal(t, c.forbidden, len(forbiddenSQLLiterals(expr)) > 0)
		})
	}
}

// constantSQLString joins only literal + literal expressions, including
// parentheses. It catches "system." + "processes" without conflating separate
// arguments or unrelated literals. SQL assembled through variables, named
// constants or function calls is outside this syntactic check's scope.
func constantSQLString(expr ast.Expr) (string, bool) {
	switch expr := expr.(type) {
	case *ast.BasicLit:
		if expr.Kind == token.STRING {
			value, err := strconv.Unquote(expr.Value)
			return value, err == nil
		}
	case *ast.ParenExpr:
		return constantSQLString(expr.X)
	case *ast.BinaryExpr:
		if expr.Op == token.ADD {
			left, leftOK := constantSQLString(expr.X)
			right, rightOK := constantSQLString(expr.Y)
			return left + right, leftOK && rightOK
		}
	}
	return "", false
}
