package clickhousequery_test

import (
	"bytes"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
)

type sqlCase struct {
	Name       string `json:"name"`
	SQL        string `json:"sql"`
	Verdict    string `json:"verdict"`
	Clause     string `json:"clause,omitempty"`
	Normalized string `json:"normalized,omitempty"`
}

func TestQueryGuard_SharedCorpus(t *testing.T) {
	raw, err := os.ReadFile("testdata/audience-sql.json")
	require.NoError(t, err)
	var f struct {
		Version int       `json:"version"`
		Cases   []sqlCase `json:"cases"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(&f))
	require.Equal(t, 1, f.Version)
	require.NotEmpty(t, f.Cases)
	require.True(t, slices.IsSortedFunc(f.Cases, func(a, b sqlCase) int { return strings.Compare(a.Name, b.Name) }))
	clauses := map[string]bool{}
	accepts := 0
	for _, c := range f.Cases {
		out, err := clickhousequery.CheckAudienceSQL(c.SQL)
		if c.Verdict == "accept" {
			require.NoError(t, err, c.Name)
			require.Equal(t, c.Normalized, out, c.Name)
			require.Empty(t, c.Clause, c.Name)
			accepts++
			continue
		}
		require.Equal(t, "refuse", c.Verdict, c.Name)
		require.Empty(t, c.Normalized, c.Name)
		d, ok := clickhousequery.Describe(err)
		require.True(t, ok, c.Name)
		require.Equal(t, "CH_QUERY_INVALID", d.Code, c.Name)
		require.Equal(t, c.Clause, d.Field, c.Name)
		clauses[c.Clause] = true
	}
	require.Positive(t, accepts)
	for _, want := range []string{"SETTINGS", "PREWHERE", "FINAL", "SAMPLE", "LIMIT BY", "ARRAY JOIN", "TABLE FUNCTION", "INTO OUTFILE", "FORMAT", "MULTIPLE STATEMENTS"} {
		require.True(t, clauses[want], "the corpus refuses %s at least once", want)
	}
	for c := range clauses {
		require.NotContains(t, []string{"NOT SELECT", "SYNTAX", "SIZE"}, c, "Go-only clause names stay out of the shared corpus")
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	require.NoError(t, enc.Encode(f))
	require.Equal(t, buf.String(), string(raw), "2-space indent, LF, trailing newline")
}

func TestQueryGuard_LexerStates(t *testing.T) {
	for sql, clause := range map[string]string{
		`SELECT 'it\'s FINAL' FROM db.t`:             "",
		`SELECT 'a''b' AS x FROM db.t FORMAT JSON`:   "FORMAT",
		"SELECT id /* SETTINGS */ FROM db.t":         "",
		"SELECT id -- FORMAT\nFROM db.t":             "",
		"SELECT \"LIMIT BY\" FROM db.t":              "",
		"SELECT id /* a /* b */ SAMPLE */ FROM db.t": "",
		"SELECT $$ FINAL $$ AS x FROM db.t":          "",
		"SELECT id // SAMPLE\nFROM db.t":             "",
		"SELECT `a``FINAL` FROM db.t":                "",
		"SELECT id FROM db.t; -- tail":               "MULTIPLE STATEMENTS", // a comment after the semicolon is still a second statement
		"SELECT id FROM db.t WHERE x IN (1, 2), 3":   "SYNTAX",              // depth-0 comma outside a FROM list
		"INSERT INTO db.t SELECT 1":                  "NOT SELECT",
		"SELECT 'open":                               "SYNTAX",
		"SELECT id FROM db.t /* open":                "SYNTAX",
		"":                                           "NOT SELECT",
		"-- only a comment":                          "NOT SELECT",
	} {
		out, err := clickhousequery.CheckAudienceSQL(sql)
		if clause == "" {
			require.NoError(t, err, sql)
			require.Equal(t, sql, out, "the guard changes nothing but the terminal semicolon")
			continue
		}
		d, ok := clickhousequery.Describe(err)
		require.True(t, ok, sql)
		require.Equal(t, "CH_QUERY_INVALID", d.Code, sql)
		require.Equal(t, clause, d.Field, sql)
	}
}

// TestQueryGuard_TableFunctionForms covers spellings that the ClickHouse
// parser turns into the same tree as a refused corpus row: parentheses around
// the IN operand, the function form of IN, and qualified or commented names.
func TestQueryGuard_TableFunctionForms(t *testing.T) {
	for _, sql := range []string{
		"SELECT id FROM db.users WHERE id IN (file('x', CSV, 'id UInt64'))",
		"SELECT id FROM db.users WHERE id IN ((url('https://example.test/x')))",
		"SELECT id FROM db.users WHERE in(id, s3('https://example.test/x'))",
		"SELECT id FROM db.users WHERE globalNotIn(id, (url('https://example.test/x')))",
		"SELECT id FROM db.users WHERE NOT nullIn(id, remote('h', db.t))",
		"SELECT id FROM db.users WHERE id IN db.url('x')",
		"SELECT id FROM db.users WHERE id IN /* c */ url /* c */ ('x')",
		"SELECT id FROM db.users JOIN `db`.`s3`('x') AS b USING id",
		"SELECT id FROM db.users AS a LEFT JOIN db.o AS o ON a.id = o.id, url('x') AS b",
		"SELECT id FROM db.users WHERE id IN (SELECT id FROM url('x'))",
		"SELECT id FROM db.users UNION ALL SELECT id FROM s3('x')",
		"SELECT id FROM db.users WHERE id IN (SELECT id FROM db.vip, input('id UInt64'))",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		d, ok := clickhousequery.Describe(err)
		require.True(t, ok, sql)
		require.Equal(t, "TABLE FUNCTION", d.Field, sql)
	}
	for _, sql := range []string{
		"SELECT id FROM db.users WHERE id IN (lower('a'), 'b', 'c')",
		"SELECT id FROM db.users WHERE id IN (toUInt64(1) + 1, 2)",
		"SELECT id FROM db.users WHERE in(id, (1, 2))",
		"SELECT min(id) FROM db.users WHERE id IN (1, 2)",
		"SELECT id FROM db.users AS a JOIN db.o AS o USING (id)",
	} {
		out, err := clickhousequery.CheckAudienceSQL(sql)
		require.NoError(t, err, sql)
		require.Equal(t, sql, out)
	}
}

// TestQueryGuard_FromInFunctionArguments covers FROM inside a function call's
// argument list. It is part of the call, not a FROM clause, unless the
// argument is a query: ClickHouse also reads a query that starts with FROM.
func TestQueryGuard_FromInFunctionArguments(t *testing.T) {
	for _, sql := range []string{
		"SELECT EXTRACT(YEAR FROM now()) AS y FROM db.users",
		"SELECT substring(email FROM 2 FOR 3) AS s FROM db.users",
		"SELECT trim(BOTH ' ' FROM lower(email)) AS e FROM db.users",
		"SELECT id FROM db.users WHERE EXTRACT(YEAR FROM toDate(created_at)) = 2026",
	} {
		out, err := clickhousequery.CheckAudienceSQL(sql)
		require.NoError(t, err, sql)
		require.Equal(t, sql, out)
	}
	for _, sql := range []string{
		"SELECT exists(SELECT 1 FROM url('https://example.test/x')) AS e FROM db.users",
		"SELECT has((SELECT groupArray(id) FROM s3('x')), 1) AS h FROM db.users",
		"SELECT exists(WITH 1 AS x SELECT x FROM url('x')) AS e FROM db.users",
		"SELECT exists(FROM url('https://example.test/x') SELECT 1) AS e FROM db.users",
		"SELECT id FROM db.users WHERE id IN (FROM url('x') SELECT id)",
		"SELECT f(1, (SELECT id FROM db.t, url('x'))) FROM db.users",
		"WITH a AS (FROM url('x') SELECT id) SELECT id FROM a",
		"SELECT id FROM db.users WHERE id = any((FROM s3('x') SELECT id))",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		d, ok := clickhousequery.Describe(err)
		require.True(t, ok, sql)
		require.Equal(t, "TABLE FUNCTION", d.Field, sql)
	}
}

// TestQueryGuard_InOperandShapes pins what may follow IN: a subquery, a
// literal list or a table name passes; any call fails, whatever its name.
func TestQueryGuard_InOperandShapes(t *testing.T) {
	// A subquery, a literal list, a tuple of two or more values or a table
	// name passes after IN. A lone call fails whatever its name, with any
	// number of parentheses, because the parser drops them.
	for _, sql := range []string{
		"SELECT id FROM db.users WHERE id IN (SELECT id FROM db.vip)",
		"SELECT id FROM db.users WHERE id IN (WITH 1 AS x SELECT x)",
		"SELECT id FROM db.users WHERE id IN (1, 2)",
		"SELECT id FROM db.users WHERE id IN ((1), (2))",
		"SELECT id FROM db.users WHERE id IN db.vip",
		"SELECT id FROM db.users WHERE id NOT IN `db`.`vip`",
		"SELECT id FROM db.users WHERE id IN (CAST(1 AS UInt8), CAST(2 AS UInt8))",
		"SELECT id FROM db.users WHERE id NOT IN ('gold')",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		require.NoError(t, err, sql)
	}
	for _, sql := range []string{
		"SELECT id FROM db.users WHERE id IN numbers_mt(10)",
		"SELECT id FROM db.users WHERE id GLOBAL IN customFunction(1)",
		"SELECT id FROM db.users WHERE id NOT IN `anything`(1)",
		"SELECT id FROM db.users WHERE id IN db.anything(1)",
		"SELECT id FROM db.users WHERE id GLOBAL NOT IN ((anything(1)))",
		"SELECT id FROM db.users WHERE id IN (CAST(1 AS UInt8))",
		"SELECT id FROM db.users WHERE id NOT IN (((CAST('gold' AS String))))",
		"SELECT id FROM db.users WHERE id IN (1, file('x'))",
		"SELECT id FROM db.users WHERE globalIn(id, anything(1))",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		d, ok := clickhousequery.Describe(err)
		require.True(t, ok, sql)
		require.Equal(t, "TABLE FUNCTION", d.Field, sql)
	}
}

func TestQueryGuard_RefusedClauseForms(t *testing.T) {
	for sql, clause := range map[string]string{
		"SELECT id FROM db.users AS u GLOBAL ANY LEFT ARRAY JOIN u.tags AS t":  "ARRAY JOIN",
		"SELECT id FROM db.users ARRAY /* c */ JOIN tags":                      "ARRAY JOIN",
		"SELECT id FROM db.users LIMIT 1, 2 BY id":                             "LIMIT BY",
		"SELECT id FROM db.users ORDER BY id LIMIT 1 BY id":                    "LIMIT BY",
		"select id from db.users final":                                        "FINAL",
		"SELECT id FROM db.users UNION ALL SELECT id FROM db.v SETTINGS a = 1": "SETTINGS",
		"SELECT 1 AS final FROM db.users":                                      "FINAL", // a bare keyword alias fails closed
		"(SELECT id FROM db.users)":                                            "NOT SELECT",
		"SHOW TABLES":                                                          "NOT SELECT",
		"SELECT 1;;":                                                           "MULTIPLE STATEMENTS",
		"SELECT id FROM db.users HAVING a, b":                                  "SYNTAX",
		"SELECT café FROM db.users":                                            "SYNTAX",
		"SELECT (1] FROM db.users":                                             "SYNTAX",
		"SELECT 1) FROM db.users":                                              "SYNTAX",
		"SELECT (1 FROM db.users":                                              "SYNTAX",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		d, ok := clickhousequery.Describe(err)
		require.True(t, ok, sql)
		require.Equal(t, clause, d.Field, sql)
	}
}

func TestQueryGuard_SizeBound(t *testing.T) {
	require.Equal(t, 245760, clickhousequery.MaxAudienceSQLBytes)
	head := "SELECT id FROM db.t WHERE id IN ("
	ok := head + strings.Repeat("1,", (clickhousequery.MaxAudienceSQLBytes-len(head)-2)/2) + "1)"
	_, err := clickhousequery.CheckAudienceSQL(ok)
	require.NoError(t, err)
	_, err = clickhousequery.CheckAudienceSQL(ok + strings.Repeat(" ", clickhousequery.MaxAudienceSQLBytes))
	d, _ := clickhousequery.Describe(err)
	require.Equal(t, "SIZE", d.Field)
	exact := "SELECT id FROM db.t" + strings.Repeat(" ", clickhousequery.MaxAudienceSQLBytes-len("SELECT id FROM db.t"))
	_, err = clickhousequery.CheckAudienceSQL(exact)
	require.NoError(t, err, "the bound is inclusive")
	_, err = clickhousequery.CheckAudienceSQL(exact + " ")
	d, _ = clickhousequery.Describe(err)
	require.Equal(t, "SIZE", d.Field)
}

// TestQueryGuard_WideListsStayLinear runs the widest lists, bracket nests and
// IN chains that fit the size bound. A scan that rescans the text per comma
// or per IN is quadratic on these inputs.
func TestQueryGuard_WideListsStayLinear(t *testing.T) {
	n := (clickhousequery.MaxAudienceSQLBytes - 64) / 2
	sel := "SELECT 1" + strings.Repeat(",1", n) + " FROM db.t"
	_, err := clickhousequery.CheckAudienceSQL(sel)
	require.NoError(t, err)
	from := "SELECT 1 FROM t" + strings.Repeat(",t", n)
	_, err = clickhousequery.CheckAudienceSQL(from)
	require.NoError(t, err)
	nested := "SELECT " + strings.Repeat("(", n/2) + "1" + strings.Repeat(")", n/2) + " FROM db.t"
	_, err = clickhousequery.CheckAudienceSQL(nested)
	require.NoError(t, err)
	m := (clickhousequery.MaxAudienceSQLBytes - 64) / 8
	chain := "SELECT 1 FROM db.t WHERE " + strings.Repeat("in(", m) + "1" + strings.Repeat(", 2)", m)
	_, err = clickhousequery.CheckAudienceSQL(chain)
	require.NoError(t, err)
	chain = "SELECT 1 FROM db.t WHERE " + strings.Repeat("1 IN (", m) + "1" + strings.Repeat(")", m)
	_, err = clickhousequery.CheckAudienceSQL(chain)
	require.NoError(t, err)
	parens := "SELECT 1 FROM db.t WHERE 1 IN " + strings.Repeat("(", n/2) + "1" + strings.Repeat(")", n/2)
	_, err = clickhousequery.CheckAudienceSQL(parens)
	require.NoError(t, err)
}

func TestQueryGuard_ErrorTextIsBounded(t *testing.T) {
	_, err := clickhousequery.CheckAudienceSQL("SELECT 'secret-value' FROM db.t FINAL")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret-value", "the error never echoes the query text")
}

// TestQueryGuard_ScalarFile pins the scalar file() function, which reads a
// server file under user_files_path from any position in the query.
func TestQueryGuard_ScalarFile(t *testing.T) {
	for _, sql := range []string{
		"SELECT file('x') AS f FROM db.users",
		"SELECT id FROM db.users WHERE c = file('x')",
		"SELECT FILE ('x') AS f FROM db.users",
		"SELECT `file`('x') AS f FROM db.users",
		"SELECT \"File\"('x') AS f FROM db.users",
		"SELECT \u201cfile\u201d('x') AS f FROM db.users",
		"SELECT `\\x66ile`('x') AS f FROM db.users",
		"SELECT lower(file('x')) AS f FROM db.users",
		"SELECT id FROM db.users GROUP BY id HAVING max(c) = file('x')",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		d, ok := clickhousequery.Describe(err)
		require.True(t, ok, sql)
		require.Equal(t, "TABLE FUNCTION", d.Field, sql)
	}
	for _, sql := range []string{
		"SELECT file FROM db.users",
		"SELECT u.file AS f FROM db.users AS u",
		"SELECT 'file(x)' AS f FROM db.users",
		"SELECT filesize FROM db.users",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		require.NoError(t, err, sql)
	}
}

// TestQueryGuard_ParenthesisedJoinLists pins table functions inside a
// parenthesised FROM or JOIN list, which ClickHouse runs as a join.
func TestQueryGuard_ParenthesisedJoinLists(t *testing.T) {
	for _, sql := range []string{
		"SELECT count() FROM ((url('x') AS a CROSS JOIN system.one AS b))",
		"SELECT count() FROM (system.one AS a, (numbers(2)) AS b)",
		"SELECT count() FROM ((system.one AS a, numbers(2) AS b))",
		"SELECT count() FROM db.t AS t JOIN (s3('x') AS a CROSS JOIN system.one AS b) ON 1",
		"SELECT count() FROM (system.one AS a JOIN (url('x') AS c JOIN system.one AS d ON 1) ON 1)",
		"SELECT count() FROM ((SELECT 1 AS id) AS a, url('x') AS b)",
		"SELECT count() FROM (((SELECT 1 AS id)) AS a, url('x') AS b)",
		"SELECT count() FROM ((SELECT 1 AS id) AS a JOIN url('x') AS b ON 1)",
		// Names outside the catalog test the structural list check on its own.
		"SELECT count() FROM (someTableFn(1) AS a, db.t AS b)",
		"SELECT count() FROM (db.t AS b, someTableFn(1) AS a)",
		"SELECT count() FROM ((db.t AS b, (someTableFn(1)) AS a))",
		"SELECT count() FROM ((SELECT 1 AS id) AS a, someTableFn(1) AS b)",
		"SELECT count() FROM (someTableFn(1) AS a CROSS JOIN db.t AS b)",
		"SELECT count() FROM db.t AS t JOIN (someTableFn(1) AS a CROSS JOIN db.u AS b) ON 1",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		d, ok := clickhousequery.Describe(err)
		require.True(t, ok, sql)
		require.Equal(t, "TABLE FUNCTION", d.Field, sql)
	}
	for _, sql := range []string{
		"SELECT a.id FROM (db.a AS a JOIN db.b AS b ON a.id = b.id)",
		"SELECT a.id FROM (db.a AS a, db.b AS b)",
		"SELECT s.id FROM (SELECT id FROM db.users) AS s",
		"SELECT s.id FROM ((SELECT id FROM db.users)) AS s",
		"SELECT a.id FROM ((SELECT id FROM db.users) AS a, db.orders AS b)",
		"SELECT id FROM db.users WHERE (a, b) IN (SELECT a, b FROM db.t)",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		require.NoError(t, err, sql)
	}
}

// TestQueryGuard_TableFunctionNamesAnywhere pins the name rule: a call of any
// 26.3 table function is refused wherever it stands.
func TestQueryGuard_TableFunctionNamesAnywhere(t *testing.T) {
	for _, sql := range []string{
		"SELECT id FROM db.users WHERE `in`(id, url('x'))",
		"SELECT id FROM db.users WHERE \"IN\"(id, s3('x'))",
		"SELECT lower(toString(remoteSecure('h', db.t))) AS r FROM db.users",
		"SELECT id FROM db.users WHERE has(arrayMap(x -> x, [1]), iceberg('x'))",
		"SELECT id FROM db.users ORDER BY mongodb('h', 'db', 'c', 'u', 'p')",
		"SELECT id FROM db.users WHERE id IN (1, numbers_mt(3))",
		"SELECT `\\x75rl`('x') AS u FROM db.users",
		"SELECT \u201cUrlCluster\u201d('c', 'x') AS u FROM db.users",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		d, ok := clickhousequery.Describe(err)
		require.True(t, ok, sql)
		require.Equal(t, "TABLE FUNCTION", d.Field, sql)
	}
	for _, sql := range []string{
		"SELECT url, file, remote, numbers FROM db.users",
		"SELECT encodeURLComponent(url) AS u, extractURLParameter(url, 'a') AS a FROM db.users",
		"SELECT u.url AS url FROM db.users AS u WHERE url = 'https://example.test/'",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		require.NoError(t, err, sql)
	}
}

// TestQueryGuard_PinnedTableFunctionCatalog checks the name rule against the
// table function list of the pinned 26.3 image
// (SELECT name FROM system.table_functions), kept apart from the code: every
// listed name is refused, and the shared corpus has a refusal row for it.
func TestQueryGuard_PinnedTableFunctionCatalog(t *testing.T) {
	raw, err := os.ReadFile("testdata/table-functions-26.3.txt")
	require.NoError(t, err)
	names := strings.Fields(string(raw))
	require.Greater(t, len(names), 80, "the pinned catalog is complete")
	corpus, err := os.ReadFile("testdata/audience-sql.json")
	require.NoError(t, err)
	var c struct {
		Cases []sqlCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(corpus, &c))
	rows := map[string]sqlCase{}
	for _, r := range c.Cases {
		rows[r.Name] = r
	}
	for _, name := range names {
		_, err := clickhousequery.CheckAudienceSQL("SELECT " + name + "('x') AS v FROM db.users")
		d, ok := clickhousequery.Describe(err)
		require.True(t, ok, name)
		require.Equal(t, "TABLE FUNCTION", d.Field, name)
		row, ok := rows["refuse_table_function_name_"+strings.ToLower(name)]
		require.True(t, ok, "corpus row for %s", name)
		require.Equal(t, "TABLE FUNCTION", row.Clause, name)
	}
}

// TestQueryGuard_InSpellings pins the two-argument rule to the function
// spelling in(x, set); the operator spelling x IN (a, b) is a tuple.
func TestQueryGuard_InSpellings(t *testing.T) {
	for _, sql := range []string{
		"SELECT id FROM db.users WHERE in(id, someTable(1))",
		"SELECT id FROM db.users WHERE id = 1 AND notIn(id, someTable(1))",
		"SELECT id FROM db.users WHERE (in(id, someTable(1)))",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		d, ok := clickhousequery.Describe(err)
		require.True(t, ok, sql)
		require.Equal(t, "TABLE FUNCTION", d.Field, sql)
	}
	for _, sql := range []string{
		"SELECT id FROM db.users WHERE id IN (1, lower('a'))",
		"SELECT id FROM db.users WHERE id GLOBAL NOT IN (CAST(1 AS UInt8), CAST(2 AS UInt8))",
		"SELECT id FROM db.users WHERE (id) IN (CAST(1 AS UInt8), CAST(2 AS UInt8))",
		"SELECT id FROM db.users WHERE {p:UInt8} IN (CAST(1 AS UInt8), CAST(2 AS UInt8))",
	} {
		_, err := clickhousequery.CheckAudienceSQL(sql)
		require.NoError(t, err, sql)
	}
}
