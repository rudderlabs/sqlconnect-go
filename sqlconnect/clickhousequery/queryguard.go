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
			if inFrom[tk.Depth] && g.tableFunctionAt(i+1) >= 0 {
				return "TABLE FUNCTION"
			}
			if !inFrom[tk.Depth] && tk.Depth == 0 && !listOK {
				return "SYNTAX"
			}
		case tk.Kind == chsql.Punct && (tk.Text == ")" || tk.Text == "]" || tk.Text == "}"):
			delete(inFrom, tk.Depth) // a closer carries the inner depth
			open = open[:len(open)-1]
		case isFileCall(tk, g.at(i-1), g.at(i+1)):
			// The scalar file() reads a server file under user_files_path in
			// any position, like the file() table function after FROM.
			return "TABLE FUNCTION"
		case tk.Kind != chsql.Word:
		case tk.Upper == "FROM" && len(open) > 0 && open[len(open)-1].args:
		case tk.Upper == "FROM" || tk.Upper == "JOIN":
			inFrom[tk.Depth] = true
			if g.tableFunctionAt(i+1) >= 0 {
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
		case inFunctions[tk.Upper] && g.inReadsTableFunction(i):
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

// isFileCall reports a call of the file function: a bare or quoted name
// "file" in any case, not qualified by a dot, followed by "(". A quoted call
// name that holds an escape also counts.
func isFileCall(tk, prev, next chsql.Token) bool {
	if !isOpener(next) || (prev.Kind == chsql.Punct && prev.Text == ".") {
		return false
	}
	switch tk.Kind {
	case chsql.Word:
		return tk.Upper == "FILE"
	case chsql.QuotedIdent:
		// A quoted call name with an escape could spell file, so it fails
		// closed too.
		name := strings.Trim(tk.Text, "`\"\u201c\u201d")
		return strings.EqualFold(name, "file") || strings.Contains(name, "\\")
	}
	return false
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
func (g *guard) inReadsTableFunction(i int) bool {
	if g.wrappedTableFunction(i+1) >= 0 {
		return true
	}
	if !isOpener(g.at(i + 1)) {
		return false
	}
	c := g.comma[i+1]
	if c == 0 {
		return false
	}
	last := g.wrappedTableFunction(c + 1)
	return last >= 0 && last+1 == g.closerOf(i+1)
}
