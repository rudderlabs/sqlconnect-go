package clickhousequery_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
)

func p09Names(t testing.TB) []string {
	t.Helper()
	raw, err := os.ReadFile("testdata/table-functions-26.3.txt")
	if err != nil {
		t.Fatal(err)
	}
	names := strings.Fields(string(raw))
	if len(names) < 80 {
		t.Fatal("incomplete independent table-function fixture")
	}
	return names
}

func p09Refused(t testing.TB, sql, clause string) {
	t.Helper()
	out, err := clickhousequery.CheckAudienceSQL(sql)
	d, ok := clickhousequery.Describe(err)
	if !ok || d.Code != "CH_QUERY_INVALID" || d.Field != clause || out != "" {
		t.Fatalf("sql=%q: out=%q err=%v details=%+v; want %s", sql, out, err, d, clause)
	}
}

func TestGuardCatalogVariants(t *testing.T) {
	for _, name := range p09Names(t) {
		t.Run(name, func(t *testing.T) {
			for _, callName := range []string{name, strings.ToUpper(name), "`" + name + "`", "\"" + name + "\"", "“" + name + "”", "db." + name} {
				for _, gap := range []string{"", "\t", "/* outer /* inner */ tail */", "-- note\n", "// note\n", "# note\n"} {
					call := callName + gap + "('x')"
					for _, sql := range []string{"SELECT " + call, "SELECT lower(toString(" + call + ")) FROM db.t", "SELECT 1 FROM db.t WHERE id IN (1, " + call + ")"} {
						p09Refused(t, sql, "TABLE FUNCTION")
					}
				}
			}
			// Names inside inert tokens and longer scalar names are not calls of the pinned name.
			for _, sql := range []string{"SELECT '" + name + "(x)'", "SELECT $$" + name + "(x)$$", "SELECT `" + name + "` FROM db.t", "SELECT " + name + "Suffix(1)", "SELECT 1 /* " + name + "(x) */"} {
				out, err := clickhousequery.CheckAudienceSQL(sql)
				if err != nil || out != sql {
					t.Fatalf("inert/longer name %q: out=%q err=%v", sql, out, err)
				}
			}
		})
	}
}

func TestGuardBoundaryAndStructure(t *testing.T) {
	for _, sql := range []string{"SELECT ([1)]", "SELECT {x", "SELECT 1)", "SELECT [1", "SELECT (1]", "SELECT 1 /* outer /* inner */"} {
		p09Refused(t, sql, "SYNTAX")
	}
	for _, suffix := range []string{";", "; SELECT 2", "; -- comment", "; /* comment */", "; // comment", "; # comment"} {
		p09Refused(t, "SELECT 1;"+suffix, "MULTIPLE STATEMENTS")
	}
	for _, sql := range []string{"SELECT 1; -- comment", "SELECT 1; /* comment */", "SELECT 1; // comment", "SELECT 1; # comment"} {
		p09Refused(t, sql, "MULTIPLE STATEMENTS")
	}
	for _, sql := range []string{"SELECT `safe\\name`(1)", "SELECT \"safe\\name\"(1)"} {
		p09Refused(t, sql, "TABLE FUNCTION")
	}
	// Multibyte contents make a character-count regression observably different from a byte bound.
	head := "SELECT '"
	pad := clickhousequery.MaxAudienceSQLBytes - len(head) - 1
	exact := head + strings.Repeat("é", pad/2) + strings.Repeat("x", pad%2) + "'"
	if out, err := clickhousequery.CheckAudienceSQL(exact); err != nil || out != exact {
		t.Fatalf("exact byte limit rejected: %v", err)
	}
	p09Refused(t, exact+" ", "SIZE")
}

func TestGuardNormalization(t *testing.T) {
	for input, want := range map[string]string{
		"  SELECT 1 \t; \n":       "  SELECT 1",
		"SELECT 'é'\u0085;\u0085": "SELECT 'é'",
		"SELECT 1 /*keep*/ \t;":   "SELECT 1 /*keep*/",
		"SELECT 1 --keep\n;":      "SELECT 1 --keep",
	} {
		out, err := clickhousequery.CheckAudienceSQL(input)
		if err != nil || out != want {
			t.Fatalf("normalize %q: got %q, %v; want %q", input, out, err, want)
		}
		second, err := clickhousequery.CheckAudienceSQL(out)
		if err != nil || second != out {
			t.Fatalf("not idempotent: %q -> %q, %v", out, second, err)
		}
	}
}

func FuzzGuard(f *testing.F) {
	raw, err := os.ReadFile("testdata/audience-sql.json")
	if err != nil {
		f.Fatal(err)
	}
	var corpus struct {
		Cases []struct {
			SQL string `json:"sql"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		f.Fatal(err)
	}
	for _, row := range corpus.Cases {
		f.Add(row.SQL)
	}
	for _, sql := range []string{"", "SELECT 1 \t;", "SELECT \x00", "SELECT \xff", "SELECT (((", "SELECT $$'$$", "SELECT 1\u200b;"} {
		f.Add(sql)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		out, err := clickhousequery.CheckAudienceSQL(sql)
		if err != nil {
			d, ok := clickhousequery.Describe(err)
			if !ok || d.Code != "CH_QUERY_INVALID" || out != "" {
				t.Fatalf("unstructured refusal: out=%q err=%v", out, err)
			}
			return
		}
		if len(sql) > clickhousequery.MaxAudienceSQLBytes || !strings.HasPrefix(sql, out) {
			t.Fatalf("invalid normalization: %q -> %q", sql, out)
		}
		again, err := clickhousequery.CheckAudienceSQL(out)
		if err != nil || again != out {
			t.Fatalf("not idempotent: %q -> %q -> %q, %v", sql, out, again, err)
		}
	})
}

func FuzzBlockedCalls(f *testing.F) {
	names := p09Names(f)
	for i := range names {
		f.Add(uint16(i), uint8(0), uint8(0), uint8(0))
	}
	f.Fuzz(func(t *testing.T, index uint16, quote, gap, depth uint8) {
		name := names[int(index)%len(names)]
		forms := []string{name, strings.ToUpper(name), "`" + name + "`", "\"" + name + "\"", "“" + name + "”", "db." + name}
		gaps := []string{"", " ", "/*a/*b*/c*/", "--x\n", "//x\n", "# x\n"}
		n := int(depth % 24)
		sql := "SELECT " + strings.Repeat("toString(", n) + forms[int(quote)%len(forms)] + gaps[int(gap)%len(gaps)] + "('x')" + strings.Repeat(")", n)
		p09Refused(t, sql, "TABLE FUNCTION")
	})
}

func TestGuardRejectsIncompleteExponentAfterNormalization(t *testing.T) {
	for _, sql := range []string{"SELECT 0.E ", "SELECT 0.E;", "SELECT 0.E"} {
		p09Refused(t, sql, "SYNTAX")
	}
}
