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

// CheckAudienceSQL returns the normalised query text, or CH_QUERY_INVALID
// naming the first refused construct. The normalised text is the input less
// one terminal semicolon and the trailing whitespace; nothing else changes.
// The error never contains query text.
// The guard allows system and information_schema databases on purpose; the
// credential's grants decide access to them.
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
	message := "the audience query uses a construct that sync refuses"
	switch clause {
	case "PARENTHESIZED SELECT":
		message = "audience queries starting with a parenthesized SELECT are refused"
	case "FORMAT":
		message = "FORMAT clauses and bare columns named format are refused"
	case "TABLE FUNCTION":
		message = "table-function calls, including scalar format(), are refused"
	}
	return cherr.New(cherr.CodeQueryInvalid, clause, message)
}

// guard scans the tokens of one query without spaces and comments.
type guard struct {
	w []chsql.Token
}

var closerFor = map[string]string{"(": ")", "[": "]", "{": "}"}

// newGuard drops spaces and comments and checks that the brackets balance.
// It reports false when they do not.
func newGuard(toks []chsql.Token) (*guard, bool) {
	g := &guard{w: make([]chsql.Token, 0, len(toks))}
	var open []string
	for _, tk := range toks {
		if tk.Kind == chsql.Space || tk.Kind == chsql.Comment {
			continue
		}
		g.w = append(g.w, tk)
		if tk.Kind != chsql.Punct {
			continue
		}
		switch {
		case closerFor[tk.Text] != "":
			open = append(open, closerFor[tk.Text])
		case isCloser(tk):
			if len(open) == 0 || open[len(open)-1] != tk.Text {
				return nil, false
			}
			open = open[:len(open)-1]
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
	if len(w) > 0 && isOpener(w[0]) {
		return "PARENTHESIZED SELECT"
	}
	if len(w) == 0 || (w[0].Upper != "SELECT" && w[0].Upper != "WITH") {
		return "NOT SELECT"
	}
	for i, tk := range w {
		if tk.Kind == chsql.Word && !isOpener(g.at(i+1)) &&
			(isDot(g.at(i-1)) || isDot(g.at(i+1)) && isNameToken(g.at(i+2))) {
			// A word next to a dot names a database, table or column, as in
			// u.format, db.sample or sample.users: it is never a clause
			// keyword. Before a dot this needs a name after it, so
			// SAMPLE .5 (a dot and a number) stays the SAMPLE clause. A call
			// after a dot still goes through the name rule.
			continue
		}
		switch {
		case isTableFunctionCall(tk, g.at(i+1)):
			// A call of a 26.3 table function name, or of the scalar file(),
			// is refused in any position. This is defence in depth: the
			// connector trusts the credential's own read capabilities.
			return "TABLE FUNCTION"
		case tk.Kind != chsql.Word:
		case fromEnders[tk.Upper]:
			switch {
			case tk.Upper == "FORMAT" || tk.Upper == "SETTINGS" || tk.Upper == "PREWHERE":
				return tk.Upper
			case tk.Upper == "INTO" && g.at(i+1).Upper == "OUTFILE":
				return "INTO OUTFILE"
			}
		case tk.Upper == "FINAL" || tk.Upper == "SAMPLE" &&
			(g.at(i+1).Kind == chsql.Number || isDot(g.at(i+1)) && g.at(i+2).Kind == chsql.Number):
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
// (SELECT name FROM system.table_functions, kept in
// testdata/table-functions-26.3.txt), in lower case. A call of one of these
// names is refused in any position; file is also a scalar function that reads
// a server file. The rule is version-specific defence in depth, not a
// sandbox: the connector trusts the read capabilities of the credential, as
// on the other warehouses, so views, dictionaries, user-defined functions
// and functions added after 26.3 are not refused.
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

func isNameToken(tk chsql.Token) bool { return tk.Kind == chsql.Word || tk.Kind == chsql.QuotedIdent }

func isDot(tk chsql.Token) bool { return tk.Kind == chsql.Punct && tk.Text == "." }

func isOpener(tk chsql.Token) bool { return tk.Kind == chsql.Punct && tk.Text == "(" }

func isCloser(tk chsql.Token) bool {
	return tk.Kind == chsql.Punct && (tk.Text == ")" || tk.Text == "]" || tk.Text == "}")
}
