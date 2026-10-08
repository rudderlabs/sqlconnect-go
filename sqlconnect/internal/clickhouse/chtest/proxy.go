package chtest

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ProxyOptions configure NewProxy.
type ProxyOptions struct {
	IdleTimeout time.Duration // the proxy's server-side keep-alive timeout, 0 for the net/http default
	PlainHTTP   bool          // serve plain HTTP instead of TLS with the front certificate
	// RewriteSQL, when set, replaces the statement text (body or "query"
	// parameter) before the proxy forwards it.
	RewriteSQL func(string) string
}

// Request is one request the proxy received. Credentials are redacted.
type Request struct {
	ConnID int
	Path   string
	Query  url.Values
	// SQL is the "query" parameter when present, else the body; empty for
	// ResetBeforeBodyOnce matches. It is not redacted, so tests must not log it.
	SQL        string
	At         time.Time
	IdleBefore time.Duration // the idle gap on this connection before the request, 0 for the first one
	Header     http.Header
}

type ruleKind int

const (
	ruleInject ruleKind = iota
	ruleDrop
	ruleReset
	ruleResetAfterForward
	ruleResetBeforeBody
)

type rule struct {
	kind   ruleKind
	match  func(Request) bool
	status int
	header http.Header
	body   string
	fired  atomic.Bool
}

type connInfo struct {
	id       int
	mu       sync.Mutex
	lastDone time.Time
}

type connKey struct{}

// Proxy sits between a client and a Server's plain HTTP port and injects one-shot faults.
type Proxy struct {
	t        *testing.T
	srv      *httptest.Server
	upstream string
	client   *http.Client
	rewrite  func(string) string

	nextConn atomic.Int64
	mu       sync.Mutex
	rules    []*rule
	requests []Request
	hooks    []func(Request)
}

// NewProxy starts a proxy on 127.0.0.1 that forwards to srv's plain HTTP port.
// It serves TLS with the front certificate unless o.PlainHTTP is set.
func NewProxy(t *testing.T, srv *Server, o ProxyOptions) *Proxy {
	t.Helper()
	p := &Proxy{
		t:        t,
		upstream: "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(srv.HTTPPort)),
		client:   newLoopbackClient(),
		rewrite:  o.RewriteSQL,
	}
	p.srv = httptest.NewUnstartedServer(http.HandlerFunc(p.serve))
	p.srv.Config.IdleTimeout = o.IdleTimeout
	p.srv.Config.ConnContext = func(ctx context.Context, _ net.Conn) context.Context {
		return context.WithValue(ctx, connKey{}, &connInfo{id: int(p.nextConn.Add(1))})
	}
	if o.PlainHTTP {
		p.srv.Start()
	} else {
		p.srv.TLS = &tls.Config{Certificates: []tls.Certificate{srv.frontCert}, MinVersion: tls.VersionTLS12}
		p.srv.StartTLS()
	}
	t.Cleanup(func() {
		p.srv.CloseClientConnections()
		p.srv.Close()
		p.client.CloseIdleConnections()
	})
	return p
}

// Port returns the proxy's port on 127.0.0.1.
func (p *Proxy) Port() int {
	return p.srv.Listener.Addr().(*net.TCPAddr).Port
}

// InjectOnce answers the first matching request with status, header and body, without forwarding it.
func (p *Proxy) InjectOnce(match func(Request) bool, status int, header http.Header, body string) {
	p.add(&rule{kind: ruleInject, match: match, status: status, header: header.Clone(), body: body})
}

// DropResponseOnce forwards the first matching request, reads the whole answer,
// then closes the connection without writing a response.
func (p *Proxy) DropResponseOnce(match func(Request) bool) {
	p.add(&rule{kind: ruleDrop, match: match})
}

// ResetOnce resets the TCP connection of the first matching request before
// forwarding it: a transport error with no server work.
func (p *Proxy) ResetOnce(match func(Request) bool) {
	p.add(&rule{kind: ruleReset, match: match})
}

// ResetAfterForwardOnce forwards the first matching request, reads the whole
// answer, then resets the client TCP connection: ECONNRESET after the server
// ran the statement.
func (p *Proxy) ResetAfterForwardOnce(match func(Request) bool) {
	p.add(&rule{kind: ruleResetAfterForward, match: match})
}

// ResetBeforeBodyOnce matches when the request headers arrive, before the
// proxy reads any body byte; match sees Query and Header, and SQL is empty.
// It resets the connection while the client still writes the body, so the
// server never sees the statement.
func (p *Proxy) ResetBeforeBodyOnce(match func(Request) bool) {
	p.add(&rule{kind: ruleResetBeforeBody, match: match})
}

// Requests returns a copy of the recorded requests in arrival order.
func (p *Proxy) Requests() []Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Request, len(p.requests))
	copy(out, p.requests)
	return out
}

// OnRequest calls hook with every request whose body the proxy has read,
// before it answers or forwards the request.
func (p *Proxy) OnRequest(hook func(Request)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hooks = append(p.hooks, hook)
}

// runHooks calls the hooks without the lock, so a hook may call Requests.
func (p *Proxy) runHooks(req Request) {
	p.mu.Lock()
	hooks := slices.Clone(p.hooks)
	p.mu.Unlock()
	for _, h := range hooks {
		h(req)
	}
}

func (p *Proxy) add(r *rule) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = append(p.rules, r)
}

// take fires the first unfired rule of the wanted kinds that matches req.
// Match functions run without the lock, so they may call Requests.
func (p *Proxy) take(req Request, beforeBody bool) *rule {
	p.mu.Lock()
	rules := append([]*rule(nil), p.rules...)
	p.mu.Unlock()
	for _, r := range rules {
		if (r.kind == ruleResetBeforeBody) != beforeBody || r.fired.Load() || !r.match(req) {
			continue
		}
		if r.fired.CompareAndSwap(false, true) {
			return r
		}
	}
	return nil
}

func (p *Proxy) record(req Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, req)
}

func (p *Proxy) serve(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	ci, _ := r.Context().Value(connKey{}).(*connInfo)
	req := Request{Path: r.URL.Path, Query: redactQuery(r.URL.Query()), At: now, Header: redactHeader(r.Header)}
	if ci != nil {
		req.ConnID = ci.id
		ci.mu.Lock()
		if !ci.lastDone.IsZero() {
			req.IdleBefore = now.Sub(ci.lastDone)
		}
		ci.mu.Unlock()
		defer func() {
			ci.mu.Lock()
			ci.lastDone = time.Now()
			ci.mu.Unlock()
		}()
	}
	p.t.Logf("chtest proxy: conn=%d %s %s query_id=%s", req.ConnID, r.Method, r.URL.Path, r.URL.Query().Get("query_id"))

	if rl := p.take(req, true); rl != nil {
		p.record(req)
		p.reset(w)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "chtest proxy: read body failed", http.StatusRequestEntityTooLarge)
		return
	}
	req.SQL = r.URL.Query().Get("query")
	if req.SQL == "" {
		req.SQL = string(body)
	}
	p.record(req)
	p.runHooks(req)

	rl := p.take(req, false)
	if rl != nil {
		switch rl.kind {
		case ruleInject:
			maps.Copy(w.Header(), rl.header)
			w.WriteHeader(rl.status)
			_, _ = io.WriteString(w, rl.body)
			return
		case ruleReset:
			p.reset(w)
			return
		}
	}

	if p.rewrite != nil {
		if q := r.URL.Query(); q.Has("query") {
			q.Set("query", p.rewrite(q.Get("query")))
			r.URL.RawQuery = q.Encode()
		} else {
			body = []byte(p.rewrite(string(body)))
		}
	}
	resp, err := p.forward(r, body)
	if err != nil {
		// The error text can hold the upstream URL with the query string
		// (password, SQL) or raw bytes of a malformed upstream answer, so the
		// answer is a fixed message.
		p.t.Logf("chtest proxy: upstream request failed (%T)", err)
		http.Error(w, "chtest proxy: upstream request failed", http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if rl != nil { // ruleDrop or ruleResetAfterForward: the server ran the statement
		_, _ = io.Copy(io.Discard, resp.Body)
		if rl.kind == ruleDrop {
			if c := p.hijack(w); c != nil {
				_ = c.Close()
			}
			return
		}
		p.reset(w)
		return
	}

	maps.Copy(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32<<10)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr != nil {
			return
		}
	}
}

// forwardedHeaders are the request headers the proxy passes upstream.
var forwardedHeaders = []string{"Authorization", "Content-Type", "Content-Encoding", "Accept-Encoding", "User-Agent"}

func (p *Proxy) forward(r *http.Request, body []byte) (*http.Response, error) {
	// Build the target from the path and query only, so an absolute-form
	// request line cannot change the upstream host.
	target := p.upstream + "/" + strings.TrimLeft(r.URL.EscapedPath(), "/")
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	up, err := http.NewRequestWithContext(r.Context(), r.Method, target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, vs := range r.Header {
		if strings.HasPrefix(http.CanonicalHeaderKey(k), "X-Clickhouse-") {
			up.Header[k] = vs
		}
	}
	for _, k := range forwardedHeaders {
		if vs := r.Header.Values(k); len(vs) > 0 {
			up.Header[k] = vs
		}
	}
	return p.client.Do(up)
}

func (p *Proxy) hijack(w http.ResponseWriter) net.Conn {
	c, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		p.t.Errorf("chtest proxy: hijack: %v", err)
		return nil
	}
	return c
}

// reset closes the connection with SO_LINGER 0, which sends a TCP RST.
func (p *Proxy) reset(w http.ResponseWriter) {
	c := p.hijack(w)
	if c == nil {
		return
	}
	tc := tcpOf(c)
	if tc == nil {
		_ = c.Close()
		return
	}
	_ = tc.SetLinger(0)
	_ = tc.Close()
}

// tcpOf returns the TCP socket under c. Over HTTPS the hijacked conn is the *tls.Conn.
func tcpOf(c net.Conn) *net.TCPConn {
	if tc, ok := c.(*tls.Conn); ok {
		c = tc.NetConn()
	}
	tcp, _ := c.(*net.TCPConn)
	return tcp
}

const redacted = "[redacted]"

// maxBody caps the request body the proxy buffers.
const maxBody = 256 << 20

// secretHeaders hold credentials; the proxy never records or logs their values.
var secretHeaders = []string{"Authorization", "X-Clickhouse-Key", "Proxy-Authorization"}

func redactHeader(h http.Header) http.Header {
	out := h.Clone()
	for _, k := range secretHeaders {
		if _, ok := out[k]; ok {
			out[k] = []string{redacted}
		}
	}
	return out
}

func redactQuery(q url.Values) url.Values {
	if _, ok := q["password"]; ok {
		q["password"] = []string{redacted}
	}
	return q
}
