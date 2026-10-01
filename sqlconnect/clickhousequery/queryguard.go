package clickhousequery

import (
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chsql"
)

// MaxAudienceSQLBytes bounds the audience query text. It stays below the
// server default max_query_size (262144) and leaves room for the DESCRIBE and
// INSERT ... SELECT wrappers. Lookout applies the same number.
const MaxAudienceSQLBytes = 240 * 1024

// fromEnders are the clause keywords that end a FROM list at the same depth.
var fromEnders = map[string]bool{
	"WHERE": true, "PREWHERE": true, "GROUP": true, "HAVING": true, "ORDER": true, "LIMIT": true,
	"OFFSET": true, "SETTINGS": true, "FORMAT": true, "UNION": true, "EXCEPT": true, "INTERSECT": true,
	"WINDOW": true, "QUALIFY": true, "INTO": true, "SELECT": true,
}

// inFunctions are the function spellings of the IN operator. The parser
// builds the same tree for in(x, f()) as for x IN f().
var inFunctions = map[string]bool{
	"IN": true, "NOTIN": true, "NULLIN": true, "NOTNULLIN": true,
	"GLOBALIN": true, "GLOBALNOTIN": true, "GLOBALNULLIN": true, "GLOBALNOTNULLIN": true,
	"INIGNORESET": true, "NOTINIGNORESET": true, "NULLINIGNORESET": true, "NOTNULLINIGNORESET": true,
	"GLOBALINIGNORESET": true, "GLOBALNOTINIGNORESET": true, "GLOBALNULLINIGNORESET": true,
	"GLOBALNOTNULLINIGNORESET": true,
}

// CheckAudienceSQL returns the normalised query text, or CH_QUERY_INVALID
// naming the first refused construct. The normalised text is the input less
// one terminal semicolon and the trailing whitespace; nothing else changes.
// The error never contains query text.
func CheckAudienceSQL(sql string) (string, error) {
	if len(sql) > MaxAudienceSQLBytes {
		return "", refuse("SIZE")
	}
	// Tokenize first, so that a lexer error reads as SYNTAX and every error
	// from TrimTerminalSemicolon means a second statement.
	if _, err := chsql.Tokenize(sql); err != nil {
		return "", refuse("SYNTAX")
	}
	trimmed, err := chsql.TrimTerminalSemicolon(sql)
	if err != nil {
		return "", refuse("MULTIPLE STATEMENTS")
	}
	toks, err := chsql.Tokenize(trimmed)
	if err != nil {
		return "", refuse("SYNTAX")
	}
	g, ok := newGuard(toks)
	if !ok {
		return "", refuse("SYNTAX")
	}
	if clause := g.scan(); clause != "" {
		return "", refuse(clause)
	}
	return trimmed, nil
}

func refuse(clause string) error {
	return cherr.New(cherr.CodeQueryInvalid, clause, "the audience query uses a construct that sync refuses")
}

// guard scans the tokens of one query without spaces and comments.
type guard struct {
	w []chsql.Token
	// match maps the index of an opening bracket to its closing bracket, and
	// comma maps it to its first comma at the same depth (0 when none). Both
	// are built once, so no lookup rescans the text.
	match, comma map[int]int
}

var closerFor = map[string]string{"(": ")", "[": "]", "{": "}"}

// newGuard drops spaces and comments and pairs the brackets. It reports false
// when the brackets do not balance.
func newGuard(toks []chsql.Token) (*guard, bool) {
	g := &guard{w: make([]chsql.Token, 0, len(toks)), match: map[int]int{}, comma: map[int]int{}}
	var open []int
	for _, tk := range toks {
		if tk.Kind == chsql.Space || tk.Kind == chsql.Comment {
			continue
		}
		i := len(g.w)
		g.w = append(g.w, tk)
		if tk.Kind != chsql.Punct {
			continue
		}
		switch {
		case closerFor[tk.Text] != "":
			open = append(open, i)
		case isCloser(tk):
			if len(open) == 0 || closerFor[g.w[open[len(open)-1]].Text] != tk.Text {
				return nil, false
			}
			g.match[open[len(open)-1]] = i
			open = open[:len(open)-1]
		case tk.Text == "," && len(open) > 0:
			if top := open[len(open)-1]; g.comma[top] == 0 {
				g.comma[top] = i
			}
		}
	}
	return g, len(open) == 0
}

func (g *guard) at(i int) chsql.Token {
	if i >= 0 && i < len(g.w) {
		return g.w[i]
	}
	return chsql.Token{}
}

// scan returns the first refused clause, or "".
func (g *guard) scan() string {
	w := g.w
	if len(w) == 0 || (w[0].Upper != "SELECT" && w[0].Upper != "WITH") {
		return "NOT SELECT"
	}
	inFrom := map[int]bool{} // per nesting depth: inside a FROM list
	// open holds the enclosing brackets. args is true for a function call's
	// argument list that has held no query keyword yet: a FROM there is part
	// of the call, as in EXTRACT(YEAR FROM d), not a FROM clause.
	type bracket struct{ args bool }
	var open []bracket
	// listOK tracks the nearest depth-0 clause keyword: true after one that
	// opens a comma list (select, BY, CTE, WINDOW or LIMIT n, m list). It is
	// kept as the scan goes, so a wide list costs linear time.
	listOK := false
	for i, tk := range w {
		if tk.Kind == chsql.Word && isDot(g.at(i-1)) && !isOpener(g.at(i+1)) {
			// A word after a dot names a column or table, as in u.format or
			// db.sample: it is never a clause keyword. A call after a dot
			// still goes through the name rule below.
			continue
		}
		if tk.Kind == chsql.Word && tk.Depth == 0 {
			switch tk.Upper {
			case "SELECT", "BY", "WITH", "DISTINCT", "LIMIT", "WINDOW":
				listOK = true
			case "WHERE", "HAVING", "OFFSET", "QUALIFY":
				listOK = false
			}
		}
		if len(open) > 0 && g.startsQuery(i) {
			open[len(open)-1].args = false
		}
		switch {
		case tk.Kind == chsql.Punct && closerFor[tk.Text] != "":
			open = append(open, bracket{args: tk.Text == "(" && isName(g.at(i-1))})
		case tk.Kind == chsql.Punct && tk.Text == ",":
			if inFrom[tk.Depth] && g.fromItemReadsTableFunction(i, inFrom) {
				return "TABLE FUNCTION"
			}
			if !inFrom[tk.Depth] && tk.Depth == 0 && !listOK {
				return "SYNTAX"
			}
		case tk.Kind == chsql.Punct && (tk.Text == ")" || tk.Text == "]" || tk.Text == "}"):
			delete(inFrom, tk.Depth) // a closer carries the inner depth
			open = open[:len(open)-1]
		case isTableFunctionCall(tk, g.at(i+1)):
			// A table function call reads outside the customer's tables in
			// any position, for example as an argument of a quoted `in`().
			return "TABLE FUNCTION"
		case tk.Kind != chsql.Word:
		case tk.Upper == "FROM" && len(open) > 0 && open[len(open)-1].args:
		case tk.Upper == "FROM" || tk.Upper == "JOIN":
			inFrom[tk.Depth] = true
			if g.fromItemReadsTableFunction(i, inFrom) {
				return "TABLE FUNCTION"
			}
		case fromEnders[tk.Upper]:
			inFrom[tk.Depth] = false
			switch {
			case tk.Upper == "FORMAT" || tk.Upper == "SETTINGS" || tk.Upper == "PREWHERE":
				return tk.Upper
			case tk.Upper == "INTO" && g.at(i+1).Upper == "OUTFILE":
				return "INTO OUTFILE"
			}
		case inFunctions[tk.Upper] && g.inReadsTableFunction(i, g.inFunctionSpelling(i)):
			// x [GLOBAL] [NOT] IN <table function>(...) reads outside the
			// database like FROM url(...).
			return "TABLE FUNCTION"
		case tk.Upper == "FINAL" || tk.Upper == "SAMPLE":
			return tk.Upper
		case tk.Upper == "ARRAY" && g.at(i+1).Upper == "JOIN":
			return "ARRAY JOIN"
		case tk.Upper == "BY":
			// BY is allowed only after ORDER, GROUP and PARTITION; any other
			// BY closes LIMIT ... BY (fails closed).
			if u := g.at(i - 1).Upper; u != "ORDER" && u != "GROUP" && u != "PARTITION" {
				return "LIMIT BY"
			}
		}
	}
	return ""
}

// tableFunctionNames holds every table function of ClickHouse 26.3
// (SELECT name FROM system.table_functions), in lower case. A call of one of
// these names is refused in any position, because each reads outside the
// customer's tables or can be spelled to. file is also a scalar function that
// reads a server file.
var tableFunctionNames = map[string]bool{
	"arrowflight": true, "azureblobstorage": true, "azureblobstoragecluster": true, "cluster": true, "clusterallreplicas": true, "cosn": true,
	"deltalake": true, "deltalakeazure": true, "deltalakeazurecluster": true, "deltalakecluster": true, "deltalakelocal": true, "deltalakes3": true,
	"deltalakes3cluster": true, "dictionary": true, "executable": true, "file": true, "filecluster": true, "format": true,
	"fuzzjson": true, "fuzzquery": true, "gcs": true, "generate_series": true, "generaterandom": true, "generateseries": true,
	"hdfs": true, "hdfscluster": true, "hive": true, "hudi": true, "hudicluster": true, "iceberg": true,
	"icebergazure": true, "icebergazurecluster": true, "icebergcluster": true, "iceberghdfs": true, "iceberghdfscluster": true, "iceberglocal": true,
	"iceberglocalcluster": true, "icebergs3": true, "icebergs3cluster": true, "input": true, "jdbc": true, "loop": true,
	"merge": true, "mergetreeanalyzeindexes": true, "mergetreeanalyzeindexesuuid": true, "mergetreeindex": true, "mergetreeprojection": true, "mergetreetextindex": true,
	"mongodb": true, "mysql": true, "null": true, "numbers": true, "numbers_mt": true, "odbc": true,
	"oss": true, "paimon": true, "paimonazure": true, "paimonazurecluster": true, "paimoncluster": true, "paimonhdfs": true,
	"paimonhdfscluster": true, "paimonlocal": true, "paimons3": true, "paimons3cluster": true, "postgresql": true, "primes": true,
	"prometheusquery": true, "prometheusqueryrange": true, "redis": true, "remote": true, "remotesecure": true, "s3": true,
	"s3cluster": true, "sqlite": true, "timeseriesdata": true, "timeseriesmetrics": true, "timeseriesselector": true, "timeseriestags": true,
	"url": true, "urlcluster": true, "values": true, "view": true, "viewexplain": true, "viewifpermitted": true,
	"ytsaurus": true, "zeros": true, "zeros_mt": true,
}

// isTableFunctionCall reports a call of a table function by name, in any
// position: a bare or quoted name in any letter case, followed by "(". A
// quoted call name that holds an escape fails closed, because the escape
// could spell such a name.
func isTableFunctionCall(tk, next chsql.Token) bool {
	if !isOpener(next) {
		return false
	}
	switch tk.Kind {
	case chsql.Word:
		return tableFunctionNames[strings.ToLower(tk.Text)]
	case chsql.QuotedIdent:
		name := strings.Trim(tk.Text, "`\"\u201c\u201d")
		return tableFunctionNames[strings.ToLower(name)] || strings.Contains(name, "\\")
	}
	return false
}

func isDot(tk chsql.Token) bool { return tk.Kind == chsql.Punct && tk.Text == "." }

// exprStarters are keywords after which an expression starts, so an IN that
// follows one is the function spelling in(x, set).
var exprStarters = map[string]bool{
	"SELECT": true, "WHERE": true, "PREWHERE": true, "HAVING": true, "QUALIFY": true, "AND": true,
	"OR": true, "ON": true, "WHEN": true, "THEN": true, "ELSE": true, "CASE": true, "BY": true,
	"DISTINCT": true, "WITH": true,
}

// inFunctionSpelling reports whether the IN-family word at i is a function
// call rather than the operator. notIn, globalIn and the other joined names
// exist only as functions. A plain IN is the function when no operand ends
// right before it: after an opener, a comma, an operator or an
// expression-starting keyword. NOT and GLOBAL before IN mean the operator.
func (g *guard) inFunctionSpelling(i int) bool {
	if g.w[i].Upper != "IN" {
		return true
	}
	prev := g.at(i - 1)
	switch prev.Kind {
	case chsql.Punct:
		return !isCloser(prev)
	case chsql.Word:
		return exprStarters[prev.Upper]
	}
	return i == 0
}

func isName(tk chsql.Token) bool {
	if tk.Kind == chsql.QuotedIdent {
		return true
	}
	return tk.Kind == chsql.Word && tk.Upper != "SELECT" && tk.Upper != "WITH"
}

// startsQuery reports a query keyword at i inside brackets: SELECT or WITH
// anywhere, or FROM right after the opener, because ClickHouse also reads
// "FROM t SELECT x".
func (g *guard) startsQuery(i int) bool {
	tk := g.w[i]
	if tk.Kind != chsql.Word {
		return false
	}
	return tk.Upper == "SELECT" || tk.Upper == "WITH" || tk.Upper == "FROM" && isOpener(g.at(i-1))
}

func isOpener(tk chsql.Token) bool { return tk.Kind == chsql.Punct && tk.Text == "(" }

func isCloser(tk chsql.Token) bool {
	return tk.Kind == chsql.Punct && (tk.Text == ")" || tk.Text == "]" || tk.Text == "}")
}

// closerOf returns the index of the bracket that closes the opener at i.
func (g *guard) closerOf(i int) int { return g.match[i] }

// tableFunctionAt reports a table function call (a bare, quoted or db.name
// followed by "(") at i. It returns the index of the call's closing ")", or
// -1 when no call starts at i.
func (g *guard) tableFunctionAt(i int) int {
	switch {
	case !isName(g.at(i)):
		return -1
	case isOpener(g.at(i + 1)):
		return g.closerOf(i + 1)
	case g.at(i+1).Kind == chsql.Punct && g.at(i+1).Text == "." && isName(g.at(i+2)) && isOpener(g.at(i+3)):
		return g.closerOf(i + 3)
	}
	return -1
}

// fromItemReadsTableFunction checks the FROM list item after the FROM, JOIN
// or comma at i. ClickHouse reads a parenthesised list such as
// FROM (url(...) AS a CROSS JOIN t) as a join, so the check skips the opening
// parentheses and marks each depth it skips as a FROM list, which makes the
// commas inside it item separators too. The parenthesis right before a query
// opens a subquery, which the scan checks on its own.
func (g *guard) fromItemReadsTableFunction(i int, inFrom map[int]bool) bool {
	k := 0
	for isOpener(g.at(i + 1 + k)) {
		k++
	}
	// A query right after the openers makes the innermost one a subquery;
	// the outer ones still open join lists, as in FROM ((SELECT 1) AS a, f()).
	query := k > 0 && g.startsQuery(i+1+k)
	lists := k
	if query {
		lists = k - 1
	}
	for j := 1; j <= lists; j++ {
		inFrom[g.at(i+j).Depth] = true
	}
	return !query && g.tableFunctionAt(i+1+k) >= 0
}

// wrappedTableFunction reports a table function call at i, alone inside any
// number of parentheses. The parser drops those parentheses, so IN ((f(x)))
// builds the same tree as IN f(x). It returns the index of the last closing
// parenthesis, or -1.
func (g *guard) wrappedTableFunction(i int) int {
	k := 0
	for isOpener(g.at(i + k)) {
		k++
	}
	end := g.tableFunctionAt(i + k)
	if end < 0 {
		return -1
	}
	for n := 1; n <= k; n++ {
		if tk := g.at(end + n); tk.Kind != chsql.Punct || tk.Text != ")" {
			return -1
		}
	}
	return end + k
}

// inReadsTableFunction reports whether the IN operator or IN function at i
// takes a table function as its set: the operand after the operator, or the
// second of exactly two arguments of the function form.
func (g *guard) inReadsTableFunction(i int, function bool) bool {
	// The operator spelling x IN f(...) or x IN (a, f(...)) does not run f as
	// a table function on 26.3 (it reports UNKNOWN_FUNCTION), and a scalar
	// call there, such as CAST or lower, is a normal value. The name rule in
	// scan refuses every table function name and the scalar file() there.
	// Only the function spelling in(x, f()) gets the structural rule.
	if !function || !isOpener(g.at(i+1)) {
		return false
	}
	c := g.comma[i+1]
	if c == 0 {
		return false
	}
	last := g.wrappedTableFunction(c + 1)
	return last >= 0 && last+1 == g.closerOf(i+1)
}
