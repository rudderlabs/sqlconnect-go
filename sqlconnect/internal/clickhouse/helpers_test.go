package clickhouse_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse/chtest"
)

// requireCode asserts that err carries an adapter error with the given code.
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var ce *cherr.Error
	require.True(t, errors.As(err, &ce), "want %s, got %v", code, err)
	require.Equal(t, code, ce.Code, "%v", err)
}

// requireChainClean walks every Unwrap and Join branch of err and asserts that
// no formatting of any link contains secret.
func requireChainClean(t *testing.T, err error, secret string) {
	t.Helper()
	var walk func(e error)
	walk = func(e error) {
		if e == nil {
			return
		}
		require.NotContains(t, fmt.Sprintf("%v %+v %#v", e, e, e), secret)
		switch u := e.(type) {
		case interface{ Unwrap() []error }:
			for _, x := range u.Unwrap() {
				walk(x)
			}
		case interface{ Unwrap() error }:
			walk(u.Unwrap())
		}
	}
	walk(err)
}

// withHostPort replaces host and port in an account config.
func withHostPort(cfg json.RawMessage, host string, port int) json.RawMessage {
	m := map[string]any{}
	if err := json.Unmarshal(cfg, &m); err != nil {
		panic(err)
	}
	m["host"], m["port"] = host, port
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

// testPolicy admits the loopback fixture and the plain HTTP arm.
var testPolicy = chpolicy.Policy{AllowLoopback: true, AllowPlainHTTP: true}

// openVia opens an admin DB that connects through the proxy p.
func openVia(t *testing.T, srv *chtest.Server, p *chtest.Proxy, secure bool) *clickhouse.DB {
	t.Helper()
	return openViaWithPassword(t, srv, p, secure, srv.AdminPassword)
}

func openViaWithPassword(t *testing.T, srv *chtest.Server, p *chtest.Proxy, secure bool, password string) *clickhouse.DB {
	t.Helper()
	cfg := withHostPort(srv.Config(srv.AdminUser, password, "default", "scratch_db", secure), "localhost", p.Port())
	db, err := clickhouse.NewDBForTest(cfg, testPolicy, srv.CA)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// isHello matches the query the fork sends when it opens a connection.
func isHello(r chtest.Request) bool {
	return strings.HasPrefix(strings.TrimSpace(r.SQL), "SELECT displayName(), version(), revision(), timezone()")
}

// countingResolver answers every lookup with one address and counts the calls.
type countingResolver struct {
	answer string
	calls  atomic.Int32
}

func (r *countingResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	r.calls.Add(1)
	return []net.IPAddr{{IP: net.ParseIP(r.answer)}}, nil
}

func (r *countingResolver) Calls() int { return int(r.calls.Load()) }

// mutableResolver answers lookups for one host with an address that Set changes.
type mutableResolver struct {
	host string
	mu   sync.Mutex
	ip   string
}

func newMutableResolver(host, ip string) *mutableResolver {
	return &mutableResolver{host: host, ip: ip}
}

func (r *mutableResolver) Set(ip string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ip = ip
}

func (r *mutableResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if host != r.host {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	return []net.IPAddr{{IP: net.ParseIP(r.ip)}}, nil
}

// staticResolver maps host names to one address each.
type staticResolver map[string]string

func (r staticResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	ip, ok := r[host]
	if !ok {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	return []net.IPAddr{{IP: net.ParseIP(ip)}}, nil
}

// sniFront is a TCP relay in front of the server's HTTPS port. It reads the
// client's TLS ClientHello, reports the server name to hook, and replays the
// bytes upstream, so the server's own certificate answers the handshake.
type sniFront struct {
	ln net.Listener
}

var errPeeked = errors.New("client hello read")

func newSNIFront(t *testing.T, srv *chtest.Server, hook func(serverName string)) *sniFront {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &sniFront{ln: ln}
	upstream := net.JoinHostPort("127.0.0.1", strconv.Itoa(srv.HTTPSPort))
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() { relaySNI(c, upstream, hook) })
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return f
}

func (f *sniFront) Port() int { return f.ln.Addr().(*net.TCPAddr).Port }

func relaySNI(c net.Conn, upstream string, hook func(string)) {
	defer func() { _ = c.Close() }()
	rec := &recordingConn{Conn: c}
	_ = tls.Server(rec, &tls.Config{GetConfigForClient: func(h *tls.ClientHelloInfo) (*tls.Config, error) {
		hook(h.ServerName)
		return nil, errPeeked
	}}).Handshake()
	up, err := net.Dial("tcp", upstream)
	if err != nil {
		return
	}
	defer func() { _ = up.Close() }()
	if _, err := up.Write(rec.read.Bytes()); err != nil {
		return
	}
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, c); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
	<-done
}

// recordingConn keeps every byte read and drops every write, so the peek
// handshake sends nothing to the client.
type recordingConn struct {
	net.Conn
	read bytes.Buffer
}

func (r *recordingConn) Read(p []byte) (int, error) {
	n, err := r.Conn.Read(p)
	r.read.Write(p[:n])
	return n, err
}

func (r *recordingConn) Write(p []byte) (int, error) { return len(p), nil }

// openFloorWithServer starts the pinned 26.3 container and opens an admin DB
// over HTTPS with the fixture CA.
func openFloorWithServer(t *testing.T) (*chtest.Server, *clickhouse.DB) {
	t.Helper()
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	return srv, openAdmin(t, srv)
}

// openFloor is openFloorWithServer without the server handle.
func openFloor(t *testing.T) *clickhouse.DB {
	t.Helper()
	_, db := openFloorWithServer(t)
	return db
}

// openAdmin opens a DB as the fixture admin on the database "default".
func openAdmin(t *testing.T, srv *chtest.Server) *clickhouse.DB {
	t.Helper()
	return openWith(t, srv.Config(srv.AdminUser, srv.AdminPassword, "default", "scratch_db", true), srv)
}

// openScoped opens a DB as a user that CreateScopedUser made for customer_db
// and scratch_db.
func openScoped(t *testing.T, srv *chtest.Server, user, password string) *clickhouse.DB {
	t.Helper()
	return openWith(t, srv.Config(user, password, "customer_db", "scratch_db", true), srv)
}

func openWith(t *testing.T, cfg json.RawMessage, srv *chtest.Server) *clickhouse.DB {
	t.Helper()
	db, err := clickhouse.NewDBForTest(cfg, testPolicy, srv.CA)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedSchema creates a fresh database and runs each ";"-separated statement of
// script through db, with {{.schema}} replaced by the database name.
func seedSchema(t *testing.T, db *clickhouse.DB, script string) sqlconnect.SchemaRef {
	t.Helper()
	schema := sqlconnect.SchemaRef{Name: "seed_" + strings.ToLower(rand.String(8))}
	ctx := context.Background()
	require.NoError(t, db.CreateSchema(ctx, schema))
	t.Cleanup(func() { _ = db.DropSchema(context.Background(), schema) })
	for stmt := range strings.SplitSeq(strings.ReplaceAll(script, "{{.schema}}", db.QuoteIdentifier(schema.Name)), ";") {
		if stmt = strings.TrimSpace(stmt); stmt != "" {
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err, stmt)
		}
	}
	return schema
}

// orDefault returns v, or def when v is empty.
func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
