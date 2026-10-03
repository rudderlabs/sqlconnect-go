package clickhouse_test

import (
	"bytes"
	"context"
	"crypto/x509"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse/chtest"
)

// server starts the pinned 26.3 container for one test. Tests never run in
// parallel, so at most one container runs at a time.
func server(t *testing.T) *chtest.Server {
	t.Helper()
	return chtest.Start(t, chtest.Options{Tag: "26.3"})
}

func TestSQ25_ValidationRefusesWorkingDatabaseBeforeAnyRequest(t *testing.T) {
	srv := server(t)
	p := chtest.NewProxy(t, srv, chtest.ProxyOptions{})
	for _, name := range []string{"default", "DEFAULT", "System", "information_schema", "customer_db"} {
		cfg := withHostPort(srv.Config("rudder_retl", "pw", "customer_db", true), "localhost", p.Port())
		db, err := clickhouse.NewDBForTest(cfg, chpolicy.Policy{AllowLoopback: true}, srv.CA)
		require.NoError(t, err)
		ctx := sqlconnect.WithValidationOptions(context.Background(), sqlconnect.ValidationOptions{WorkingDatabase: name})
		_, err = db.ValidateContext(ctx)
		requireCode(t, err, "CH_CONFIG_INVALID")
		require.NoError(t, db.Close())
	}
	require.Empty(t, p.Requests(), "validation rejects the working database before any query")
}

func TestSQ26_RebindOverBothTransports(t *testing.T) {
	srv := server(t)
	for _, secure := range []bool{true, false} {
		p := chtest.NewProxy(t, srv, chtest.ProxyOptions{PlainHTTP: !secure})
		r := newMutableResolver("rebind.test", "127.0.0.1") // the fixture certificate carries SAN rebind.test
		cfg := withHostPort(srv.Config(srv.AdminUser, srv.AdminPassword, "default", secure), "rebind.test", p.Port())
		db, err := clickhouse.NewDBForTestWith(cfg, clickhouse.TestEnv{Policy: testPolicy, Roots: srv.CA, Resolver: r})
		require.NoError(t, err)
		db.SetMaxIdleConns(0) // every statement opens a new connection
		_, err = db.ExecContext(context.Background(), "SELECT 1")
		require.NoError(t, err, "secure=%v: the allowed answer connects", secure)
		sent := len(p.Requests())
		r.Set("169.254.169.254") // rebind to the metadata address
		_, err = db.ExecContext(context.Background(), "SELECT 1")
		requireCode(t, err, "CH_HOST_NOT_ALLOWED")
		require.Len(t, p.Requests(), sent, "secure=%v: no request, and so no credential, left after the rebind", secure)
		r.Set("127.0.0.1")
		_, err = db.ExecContext(context.Background(), "SELECT 1")
		require.NoError(t, err, "a later allowed answer connects again")
		_ = db.Close()
	}
}

func TestSQ5_TLSMatrix(t *testing.T) {
	srv := server(t)
	p := chtest.NewProxy(t, srv, chtest.ProxyOptions{PlainHTTP: true}) // counts plain-port requests
	try := func(host string, port int, secure bool, roots *x509.CertPool) error {
		cfg := withHostPort(srv.Config(srv.AdminUser, srv.AdminPassword, "default", secure), host, port)
		db, err := clickhouse.NewDBForTest(cfg, testPolicy, roots)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		var one uint8
		return db.QueryRowContext(context.Background(), "SELECT 1").Scan(&one)
	}
	require.NoError(t, try("localhost", srv.HTTPSPort, true, srv.CA), "HTTPS with a trusted certificate")
	requireCode(t, try("localhost", srv.HTTPSPort, true, nil), "CH_TLS")    // unknown CA: system roots only
	requireCode(t, try("127.0.0.1", srv.HTTPSPort, true, srv.CA), "CH_TLS") // hostname mismatch: no IP SAN
	requireCode(t, try("localhost", p.Port(), true, srv.CA), "CH_TLS")      // secure=true against a plain listener
	require.Empty(t, p.Requests(), "no insecure retry follows a TLS failure")
	require.NoError(t, try("localhost", srv.HTTPPort, false, nil), "plain arm under AllowPlainHTTP")
}

func TestSQ26_TLSVerifiesConfiguredHostname(t *testing.T) {
	srv := server(t)
	sni := make(chan string, 16)
	front := newSNIFront(t, srv, func(name string) { sni <- name }) // the server certificate carries SAN localhost
	resolver := staticResolver{"localhost": "127.0.0.1", "outside-san.test": "127.0.0.1"}
	for _, c := range []struct{ host, wantErr string }{{"localhost", ""}, {"outside-san.test", "CH_TLS"}} {
		cfg := withHostPort(srv.Config(srv.AdminUser, srv.AdminPassword, "default", true), c.host, front.Port())
		db, err := clickhouse.NewDBForTestWith(cfg, clickhouse.TestEnv{Policy: chpolicy.Policy{AllowLoopback: true}, Roots: srv.CA, Resolver: resolver})
		require.NoError(t, err)
		_, err = db.ExecContext(context.Background(), "SELECT 1")
		if c.wantErr == "" {
			require.NoError(t, err)
		} else {
			requireCode(t, err, c.wantErr)
		}
		require.Equal(t, c.host, <-sni, "SNI carries the configured hostname, not the pinned IP")
		_ = db.Close()
	}
}

// authFront is a plain HTTP reverse proxy to the server that records the
// scheme of each Authorization header and the URL path. It never records the
// credential itself.
type authFront struct {
	*httptest.Server
	mu      sync.Mutex
	schemes []string
	paths   []string
	keys    int
}

func newAuthFront(t *testing.T, srv *chtest.Server) *authFront {
	t.Helper()
	target, err := url.Parse("http://127.0.0.1:" + strconv.Itoa(srv.HTTPPort))
	require.NoError(t, err)
	rp := httputil.NewSingleHostReverseProxy(target)
	f := &authFront{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, _, _ := strings.Cut(r.Header.Get("Authorization"), " ")
		f.mu.Lock()
		f.schemes = append(f.schemes, scheme)
		f.paths = append(f.paths, r.URL.Path)
		if r.Header.Get("X-ClickHouse-Key") != "" || r.Header.Get("X-ClickHouse-User") != "" {
			f.keys++
		}
		f.mu.Unlock()
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *authFront) port() int {
	u, _ := url.Parse(f.URL)
	n, _ := strconv.Atoi(u.Port())
	return n
}

func TestSQ5_PlainHTTPBasicHeader(t *testing.T) {
	srv := server(t)
	f := newAuthFront(t, srv)
	cfg := withHostPort(srv.Config(srv.AdminUser, srv.AdminPassword, "default", false), "localhost", f.port())
	db, err := clickhouse.NewDBForTest(cfg, testPolicy, nil)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(context.Background(), "SELECT 1")
	require.NoError(t, err)
	f.mu.Lock()
	require.NotEmpty(t, f.schemes)
	for i, s := range f.schemes {
		require.Equal(t, "Basic", s, "plain arm sends Basic credentials")
		require.Equal(t, "/", f.paths[i], "no URL path prefix")
	}
	require.Zero(t, f.keys, "plain arm sends no X-ClickHouse credential headers")
	before := len(f.schemes)
	f.mu.Unlock()

	_, err = clickhouse.NewDBForTest(withHostPort(srv.Config("u", "p", "d", false), "localhost", f.port()), chpolicy.Policy{AllowLoopback: true}, nil)
	requireCode(t, err, "CH_CONFIG_INVALID")
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Len(t, f.schemes, before, "without AllowPlainHTTP the refusal happens before any dial")
}

func TestSQ27_TransportOptions(t *testing.T) {
	srv := server(t)
	var proxied atomic.Int32
	fakeProxy := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { proxied.Add(1) }))
	defer fakeProxy.Close()
	t.Setenv("HTTPS_PROXY", fakeProxy.URL)
	t.Setenv("HTTP_PROXY", fakeProxy.URL)
	t.Setenv("NO_PROXY", "")
	// Go never proxies localhost, so the test dials rebind.test (a SAN of the
	// fixture certificate) through a loopback resolver. net/http reads the
	// proxy variables once per process, so the Proxy == nil check below is the
	// check that does not depend on test order.
	p := chtest.NewProxy(t, srv, chtest.ProxyOptions{IdleTimeout: 10 * time.Second})
	cfg := withHostPort(srv.Config(srv.AdminUser, srv.AdminPassword, "default", true), "rebind.test", p.Port())
	db, err := clickhouse.NewDBForTestWith(cfg, clickhouse.TestEnv{
		Policy: chpolicy.Policy{AllowLoopback: true}, Roots: srv.CA, Resolver: staticResolver{"rebind.test": "127.0.0.1"},
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	for _, idle := range []time.Duration{0, 11 * time.Second, 35 * time.Second} {
		time.Sleep(idle)
		_, err := conn.ExecContext(context.Background(), "SELECT 1") // raw pinned API, unmarked context
		require.NoError(t, err, "no driver.ErrBadConn after %s idle", idle)
	}
	rows, err := db.QueryContext(context.Background(), "SELECT 1") // raw pool API
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Zero(t, proxied.Load())
	require.Nil(t, clickhouse.InspectTransport(db).Proxy)
	reqs := p.Requests()
	require.NotEmpty(t, reqs)
	for _, r := range reqs {
		require.Less(t, r.IdleBefore, 10*time.Second, "no POST on a socket the server closed after 10 s idle")
		require.Equal(t, "0", r.Query.Get("send_progress_in_http_headers"), r.SQL)
		require.True(t, strings.HasPrefix(r.Query.Get("query_id"), "retl-"), r.SQL)
	}
}

func TestSQ27_SilentInsertOutlivesShortReadTimeout(t *testing.T) {
	srv := server(t)
	srv.AdminExec(t, "CREATE TABLE default.silent (n UInt64) ENGINE = MergeTree ORDER BY n")
	run := func(readTimeout time.Duration) error {
		db, err := clickhouse.NewDBForTestWith(srv.Config(srv.AdminUser, srv.AdminPassword, "default", true),
			clickhouse.TestEnv{Policy: chpolicy.Policy{AllowLoopback: true}, Roots: srv.CA, ReadTimeout: readTimeout})
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()
		// sleepEachRow sleeps at most 3 s per block, so each row gets its own block.
		_, err = conn.ExecContext(context.Background(),
			"INSERT INTO default.silent SELECT number FROM numbers(6) WHERE sleepEachRow(1) = 0 SETTINGS max_block_size = 1")
		return err
	}
	neg := run(3 * time.Second)
	t.Logf("negative control: %v", neg)
	require.Error(t, neg, "negative control: headers arrive only at completion")
	require.NoError(t, run(0), "the production ReadTimeout (MaxRunBudget + 60 s) covers a silent 6 s INSERT")
}

func TestSQ25_SentinelEchoAtConnectionOpen(t *testing.T) {
	srv := server(t)
	var logs bytes.Buffer
	logger := log.New(&logs, "", 0)
	for _, secure := range []bool{true, false} {
		p := chtest.NewProxy(t, srv, chtest.ProxyOptions{PlainHTTP: !secure})
		p.InjectOnce(isHello, 500, http.Header{"X-ClickHouse-Exception-Code": {"516"}},
			"Code: 516. DB::Exception: sentinel-pw-7f3a is wrong. (AUTHENTICATION_FAILED)")
		db := openViaWithPassword(t, srv, p, secure, "sentinel-pw-7f3a")
		_, err := db.ExecContext(context.Background(), "SELECT 1")
		logger.Printf("%v | %+v | %#v", err, err, err)
		requireCode(t, err, "CH_AUTHENTICATION")
		requireChainClean(t, err, "sentinel-pw-7f3a")
		p.ResetOnce(func(chtest.Request) bool { return true }) // a transport error whose text could hold the URL
		_, err = db.ExecContext(context.Background(), "SELECT 1")
		logger.Printf("%v | %+v | %#v", err, err, err)
		require.Error(t, err)
		requireChainClean(t, err, "sentinel-pw-7f3a")
	}
	require.NotContains(t, logs.String(), "sentinel-pw-7f3a", "neither the server echo nor the plain-mode URL userinfo leaks")
}

func TestSQ31_RefusedDialNotReplayed(t *testing.T) {
	r := &countingResolver{answer: "169.254.169.254"}
	db, err := clickhouse.NewDBForTestWith(validJSON(nil), clickhouse.TestEnv{Resolver: r})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(context.Background(), "SELECT 1")
	requireCode(t, err, "CH_HOST_NOT_ALLOWED")
	require.Equal(t, 1, r.Calls(), "database/sql did not replay the refused dial")
}

func TestSQ30_RedirectOnHelloAndStatement(t *testing.T) {
	srv := server(t)
	srv.AdminExec(t, "CREATE DATABASE IF NOT EXISTS _rudderstack")
	srv.AdminExec(t, "CREATE TABLE _rudderstack.r (a UInt8) ENGINE = MergeTree ORDER BY a")
	var second atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { second.Add(1) }))
	defer other.Close()
	for _, c := range []struct {
		name  string
		match func(chtest.Request) bool
	}{
		{"first request", isHello},
		{"sync statement", func(r chtest.Request) bool { return strings.HasPrefix(r.SQL, "INSERT") }},
	} {
		for _, loc := range []string{"/same-origin", other.URL + "/"} {
			t.Run(c.name+" "+loc, func(t *testing.T) {
				p := chtest.NewProxy(t, srv, chtest.ProxyOptions{})
				p.InjectOnce(c.match, 308, http.Header{"Location": {loc}}, "")
				db := openVia(t, srv, p, true)
				conn, err := db.Conn(context.Background())
				if c.name == "sync statement" {
					require.NoError(t, err)
					defer func() { _ = conn.Close() }()
					_, err = conn.ExecContext(context.Background(), "INSERT INTO _rudderstack.r SELECT 1")
				}
				requireCode(t, err, "CH_REDIRECT_REFUSED")
			})
		}
	}
	require.Zero(t, second.Load(), "the second host receives zero requests")
}
