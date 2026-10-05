package clickhouse_test

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse/chtest"
)

// The production grants let the sync user read system.query_log and
// system.processes, so the guard alone keeps audience SQL inside the customer
// databases. This test lets the server judge the guard: each query the guard
// accepts runs as the scoped production user, and the server's query_log
// records what the query touched.

// guardDatabases are the customer databases the corpus names. The scoped user
// may read all of them.
var guardDatabases = []string{"db", "mysystem", "SYSTEM2", "SYSTEM", "Information_Schema", "by", "final", "sample", "Sample"}

var guardSchema = []string{
	"CREATE TABLE db.users (id Int64, email String, tags Array(String), c String, url String, s3 String, plan String," +
		" `format` String, `by` String, `file` String, `FINAL` String) ENGINE = MergeTree ORDER BY id",
	"INSERT INTO db.users VALUES (1, 'a@x', ['a', 'b'], 'p', 'https://x.test/a?a=1', 's', 'gold', 'csv', 'b', 'f', 'g')",
	"CREATE TABLE db.t (id Int64, `final` String, `settings` String, `having` String, `offset` String, `qualify` String," +
		" `sample` String, `where` String, user_id Int64) ENGINE = MergeTree ORDER BY id",
	"CREATE TABLE db.vip (id Int64) ENGINE = MergeTree ORDER BY id",
	"CREATE TABLE db.orders (id Int64) ENGINE = MergeTree ORDER BY id",
	"CREATE TABLE db.`sample` (id Int64) ENGINE = MergeTree ORDER BY id",
	"CREATE TABLE db.`system` (id Int64) ENGINE = MergeTree ORDER BY id",
	"CREATE TABLE `final`.t (id Int64) ENGINE = MergeTree ORDER BY id",
}

// reachCodes are server errors that show a query reached past the customer
// databases: a refused grant or an outbound connection.
var reachCodes = map[int]string{
	497: "ACCESS_DENIED", 198: "DNS_ERROR", 209: "SOCKET_TIMEOUT", 210: "NETWORK_ERROR",
	279: "ALL_CONNECTION_TRIES_FAILED", 519: "NO_REMOTE_SHARD_AVAILABLE", 1000: "POCO_EXCEPTION",
}

// schemaGapCodes are server errors that show the test schema lacks a table or
// database a corpus row names. UNKNOWN_IDENTIFIER is left out: the server also
// raises it for alias scoping, and the finish ratio below covers columns.
var schemaGapCodes = map[int]string{60: "UNKNOWN_TABLE", 81: "UNKNOWN_DATABASE"}

// guardProbes reach past the customer databases when the guard lets them
// through. Every address is the server itself, so a miss fails fast.
var guardProbes = []string{
	"system.query_log", "system . query_log", "`system`.`query_log`", "\"system\".processes",
	"INFORMATION_SCHEMA.tables", "information_schema.columns", "`sys\\x74em`.query_log",
	"hasColumnInTable('127.0.0.1:9000', 'system', 'one', 'dummy')",
	"HASCOLUMNINTABLE('127.0.0.1:9000', 'default', '', 'system', 'one', 'dummy')",
	"remote('127.0.0.1:9000', system.one)", "url('http://127.0.0.1:8123/', 'CSV', 'a String')",
	"file('x.csv')", "numbers(3)", "merge('system', 'query_log')", "{p:Identifier}",
	"(SELECT query FROM system.query_log)", "/* c */ system /* c */ . /* c */ processes",
}

func TestGuardOnTheServer(t *testing.T) {
	g := startGuardServer(t)
	var corpus struct {
		Cases []struct{ Name, SQL, Verdict string }
	}
	b, err := os.ReadFile("../../clickhousequery/testdata/audience-sql.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &corpus))
	require.NotEmpty(t, corpus.Cases)

	t.Run("corpus", func(t *testing.T) {
		sent := map[string]string{}
		for i, c := range corpus.Cases {
			if checked, err := clickhousequery.CheckAudienceSQL(c.SQL); err == nil {
				id := "guard-corpus-" + strconv.Itoa(i)
				sent[id] = c.Name
				g.run(t, id, checked)
			}
		}
		outcomes := g.outcomes(t, sent)
		finished := 0
		for _, id := range slices.Sorted(maps.Keys(sent)) {
			o := outcomes[id]
			requireStaysInside(t, sent[id], o)
			gap, isGap := schemaGapCodes[o.Code]
			require.False(t, isGap, "%s: the test schema lacks an object the row names (%s)", sent[id], gap)
			if o.Code == 0 {
				finished++
			}
		}
		// Most accepted rows must run end to end, or the check above proves little.
		require.GreaterOrEqual(t, finished*4, len(sent)*3, "%d of %d accepted rows finished on the server", finished, len(sent))
		t.Logf("%d of %d accepted corpus rows finished on the server", finished, len(sent))
	})

	t.Run("generated", func(t *testing.T) {
		n := 400
		if v := os.Getenv("GUARD_PROPERTY_N"); v != "" {
			n, err = strconv.Atoi(v)
			require.NoError(t, err)
		}
		var accepted []string
		for _, c := range corpus.Cases {
			if c.Verdict == "accept" {
				accepted = append(accepted, c.SQL)
			}
		}
		const seed = 20261005
		t.Logf("seed %d, %d inputs", seed, n)
		rng := rand.New(rand.NewPCG(seed, seed))
		sent := map[string]string{}
		for i := range n {
			words := strings.Split(accepted[rng.IntN(len(accepted))], " ")
			words = slices.Insert(words, 1+rng.IntN(len(words)), guardProbes[rng.IntN(len(guardProbes))])
			sql := strings.Join(words, " ")
			if checked, err := clickhousequery.CheckAudienceSQL(sql); err == nil {
				id := "guard-generated-" + strconv.Itoa(i)
				sent[id] = sql
				g.run(t, id, checked)
			}
		}
		require.NotEmpty(t, sent, "some generated inputs pass the guard, so the property runs")
		t.Logf("%d of %d generated inputs passed the guard and ran on the server", len(sent), n)
		outcomes := g.outcomes(t, sent)
		for _, id := range slices.Sorted(maps.Keys(sent)) {
			requireStaysInside(t, sent[id], outcomes[id])
		}
	})
}

type guardServer struct {
	srv        *chtest.Server
	user, pass string
}

func startGuardServer(t *testing.T) *guardServer {
	t.Helper()
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	g := &guardServer{srv: srv, user: "guard_scoped", pass: "pw_guard_scoped_0123456789"}
	srv.CreateScopedUser(t, g.user, g.pass, "db", "rudder_scratch", false)
	for _, name := range guardDatabases {
		srv.AdminExec(t, "CREATE DATABASE IF NOT EXISTS `"+name+"`")
		srv.AdminExec(t, "GRANT SELECT ON `"+name+"`.* TO "+g.user)
		if name != "db" && name != "final" {
			srv.AdminExec(t, "CREATE TABLE `"+name+"`.users (id Int64) ENGINE = MergeTree ORDER BY id")
		}
	}
	for _, stmt := range guardSchema {
		srv.AdminExec(t, stmt)
	}
	return g
}

// run sends sql as the scoped user. The customer database is the default, so
// an unqualified table resolves inside it. The outcome is read from query_log.
func (g *guardServer) run(t *testing.T, queryID, sql string) {
	t.Helper()
	// p binds a forbidden table, so an Identifier parameter that passes the
	// guard reads it and shows in query_log.
	q := url.Values{"query_id": {queryID}, "database": {"db"}, "param_e": {"a@x"}, "param_p": {"system.query_log"}, "max_execution_time": {"10"}}
	u := url.URL{Scheme: "http", Host: "127.0.0.1:" + strconv.Itoa(g.srv.HTTPPort), Path: "/", RawQuery: q.Encode()}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(sql))
	require.NoError(t, err)
	req.SetBasicAuth(g.user, g.pass)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	require.NoError(t, resp.Body.Close())
}

type guardOutcome struct {
	Code           int
	Tables         []string
	TableFunctions []string
}

// outcomes reads, as the admin, the final query_log row of each sent query.
func (g *guardServer) outcomes(t *testing.T, sent map[string]string) map[string]guardOutcome {
	t.Helper()
	g.srv.FlushLogs(t)
	ids := slices.Sorted(maps.Keys(sent))
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = "'" + id + "'"
	}
	rows := g.srv.AdminQuery(t, "SELECT query_id, exception_code, toJSONString(tables), toJSONString(used_table_functions)"+
		" FROM system.query_log WHERE type != 'QueryStart' AND query_id IN ("+strings.Join(quoted, ",")+")")
	out := map[string]guardOutcome{}
	for _, r := range rows {
		require.Len(t, r, 4, "query_log row shape")
		var o guardOutcome
		var err error
		o.Code, err = strconv.Atoi(r[1])
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal([]byte(r[2]), &o.Tables))
		require.NoError(t, json.Unmarshal([]byte(r[3]), &o.TableFunctions))
		out[r[0]] = o
	}
	for _, id := range ids {
		require.Contains(t, out, id, "the server logged %s", sent[id])
	}
	return out
}

// requireStaysInside fails when an accepted query touched anything outside the
// customer databases. system.one is the implicit table of a SELECT without FROM.
func requireStaysInside(t *testing.T, label string, o guardOutcome) {
	t.Helper()
	name, reached := reachCodes[o.Code]
	require.False(t, reached, "%s: the server answered %s, so the accepted query reached past the customer databases", label, name)
	for _, tbl := range o.Tables {
		db, _, _ := strings.Cut(tbl, ".")
		require.True(t, tbl == "system.one" || slices.Contains(guardDatabases, db), "%s: the accepted query read %s", label, tbl)
	}
	require.Empty(t, o.TableFunctions, "%s: the accepted query used a table function", label)
}
