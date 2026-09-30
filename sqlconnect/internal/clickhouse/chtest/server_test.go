package chtest_test

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse/chtest"
)

func TestFixture_Image(t *testing.T) {
	require.Equal(t, "clickhouse/clickhouse-server@sha256:810861a2e2d0188744f5f23b2d3ec9ff95812bcb9ddbb8fed13a377a7f305893", chtest.Image("26.3"))
	for _, tag := range []string{"25.8", "24.8"} {
		require.Regexp(t, `^clickhouse/clickhouse-server@sha256:[0-9a-f]{64}$`, chtest.Image(tag))
	}
	require.Panics(t, func() { chtest.Image("latest") }, "an unpinned tag has no image")
}

func TestFixture_ProxyUpstreamErrorHidesURL(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	closedPort := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	p := chtest.NewProxy(t, &chtest.Server{HTTPPort: closedPort}, chtest.ProxyOptions{PlainHTTP: true})
	q := url.Values{"password": {"pw_Sentinel_1"}, "query": {"SELECT 'lit_Sentinel_2'"}}
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/?%s", p.Port(), q.Encode()), "text/plain", strings.NewReader(""))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)
	require.Contains(t, string(b), "upstream request failed")
	require.NotContains(t, string(b), "Sentinel", "the answer holds no query string")
	require.Equal(t, "[redacted]", p.Requests()[0].Query.Get("password"))
}

func TestFixture_ProxyDoesNotFollowRedirects(t *testing.T) {
	var elsewhere atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { elsewhere.Add(1) }))
	defer other.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/steal", http.StatusTemporaryRedirect)
	}))
	defer upstream.Close()

	p := chtest.NewProxy(t, &chtest.Server{HTTPPort: upstream.Listener.Addr().(*net.TCPAddr).Port}, chtest.ProxyOptions{PlainHTTP: true})
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/", p.Port()), strings.NewReader("SELECT 1"))
	require.NoError(t, err)
	req.SetBasicAuth("u", "p")
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusTemporaryRedirect, resp.StatusCode, "the proxy passes the redirect back unfollowed")
	require.Zero(t, elsewhere.Load(), "the redirect target receives no request")
}

func TestFixture_ProxyResetModes(t *testing.T) {
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	for _, plain := range []bool{true, false} {
		p := chtest.NewProxy(t, srv, chtest.ProxyOptions{PlainHTTP: plain})
		scheme := map[bool]string{true: "http", false: "https"}[plain]
		c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: srv.CA}}}
		post := func(queryID, body string) (string, error) {
			req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s://localhost:%d/?query_id=%s", scheme, p.Port(), queryID), strings.NewReader(body))
			require.NoError(t, err)
			req.SetBasicAuth(srv.AdminUser, srv.AdminPassword)
			resp, err := c.Do(req)
			if err != nil {
				return "", err
			}
			defer func() { _ = resp.Body.Close() }()
			b, err := io.ReadAll(resp.Body)
			if err == nil && resp.StatusCode != http.StatusOK {
				err = fmt.Errorf("status %d: %s", resp.StatusCode, b)
			}
			return string(b), err
		}
		// finished counts the server-side completions of a query id.
		finished := func(queryID string) string {
			srv.FlushLogs(t)
			return srv.AdminQuery(t, fmt.Sprintf("SELECT count() FROM system.query_log WHERE query_id = '%s' AND type = 'QueryFinish'", queryID))[0][0]
		}

		afterID, againID, beforeID := "af-"+scheme, "ok-"+scheme, "bb-"+scheme
		p.ResetAfterForwardOnce(func(chtest.Request) bool { return true })
		_, err := post(afterID, "SELECT 1")
		require.ErrorIs(t, err, syscall.ECONNRESET, "%s: reset after the server answered", scheme)
		require.Equal(t, "1", finished(afterID), "%s: the server ran the statement once before the reset", scheme)
		body, err := post(againID, "SELECT 1")
		require.NoError(t, err, "one-shot rules fire once")
		require.Equal(t, "1\n", body)

		p.ResetBeforeBodyOnce(func(r chtest.Request) bool { return r.Path == "/" && r.Query.Get("query_id") == beforeID })
		_, err = post(beforeID, "SELECT 1 -- "+strings.Repeat("x", 4<<20))
		// The client's read loop can see the RST first and close the socket
		// under the body writer, which then fails with net.ErrClosed.
		require.True(t, errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, net.ErrClosed), "%s: observed %v", scheme, err)
		t.Logf("%s reset-before-body observed: %v", scheme, err)
		require.Equal(t, "0", finished(beforeID), "%s: the server never saw the statement", scheme)
		body, err = post(beforeID, "SELECT 2")
		require.NoError(t, err, "%s: the reset-before-body rule fires once", scheme)
		require.Equal(t, "2\n", body)
	}

	t.Run("scoped user, injected answers, dropped responses and redacted records", func(t *testing.T) {
		srv.CreateScopedUser(t, "rudder_retl", "pw_Retl_123", "customer_db", "scratch_db", true)
		require.Equal(t, [][]string{{"1"}}, srv.AdminQuery(t, "SELECT count() FROM system.users WHERE name = 'rudder_retl'"))
		grants := srv.AdminQuery(t, "SHOW GRANTS FOR rudder_retl")
		joined := fmt.Sprint(grants)
		for _, want := range []string{
			"GRANT SELECT ON customer_db.*",
			"GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON scratch_db.*",
			"GRANT SELECT ON system.processes",
			"GRANT SELECT ON system.query_log",
			"GRANT ALTER DELETE ON scratch_db.sync_log",
		} {
			require.Contains(t, joined, want)
		}
		srv.FlushLogs(t)

		var cfg map[string]any
		require.NoError(t, json.Unmarshal(srv.Config("rudder_retl", "pw_Retl_123", "customer_db", "scratch_db", true), &cfg))
		require.Equal(t, map[string]any{
			"host": "localhost", "port": float64(srv.HTTPSPort), "database": "customer_db", "user": "rudder_retl",
			"password": "pw_Retl_123", "secure": true, "skipVerify": false, "scratchDatabase": "scratch_db",
		}, cfg)
		require.NoError(t, json.Unmarshal(srv.Config("u", "p", "d", "s", false), &cfg))
		require.Equal(t, float64(srv.HTTPPort), cfg["port"])
		require.Regexp(t, `^sha256:[0-9a-f]{64}$`, srv.ImageDigest(t))

		p := chtest.NewProxy(t, srv, chtest.ProxyOptions{})
		c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: srv.CA}}}
		do := func(queryID, sql string) (int, string, error) {
			target := fmt.Sprintf("https://localhost:%d/?query_id=%s", p.Port(), queryID)
			// Only never-forwarded requests carry the extra credentials: ClickHouse refuses mixed auth methods.
			secrets := queryID == "inj" || queryID == "reset"
			if secrets {
				target += "&password=secret"
			}
			req, err := http.NewRequest(http.MethodPost, target, strings.NewReader(sql))
			require.NoError(t, err)
			req.SetBasicAuth("rudder_retl", "pw_Retl_123")
			if secrets {
				req.Header.Set("X-ClickHouse-Key", "secret")
			}
			resp, err := c.Do(req)
			if err != nil {
				return 0, "", err
			}
			defer func() { _ = resp.Body.Close() }()
			b, err := io.ReadAll(resp.Body)
			return resp.StatusCode, string(b), err
		}

		status, body, err := do("ok", "SELECT 42")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status, body)
		require.Equal(t, "42\n", body)

		p.InjectOnce(func(r chtest.Request) bool { return strings.HasPrefix(r.SQL, "SELECT 7") }, http.StatusServiceUnavailable, http.Header{"X-Test": {"1"}}, "injected")
		status, body, err = do("inj", "SELECT 7")
		require.NoError(t, err)
		require.Equal(t, http.StatusServiceUnavailable, status)
		require.Equal(t, "injected", body)

		p.DropResponseOnce(func(r chtest.Request) bool { return r.Query.Get("query_id") == "drop" })
		_, _, err = do("drop", "SELECT 8")
		require.Error(t, err, "the proxy closes the connection without an answer")

		p.ResetOnce(func(r chtest.Request) bool { return r.Query.Get("query_id") == "reset" })
		_, _, err = do("reset", "SELECT 9")
		require.ErrorIs(t, err, syscall.ECONNRESET)

		reqs := p.Requests()
		require.Len(t, reqs, 4)
		require.Equal(t, "SELECT 42", reqs[0].SQL)
		for _, r := range reqs {
			require.NotContains(t, fmt.Sprint(r.Header), "secret", "recorded headers hold no credentials")
			require.NotContains(t, fmt.Sprint(r.Header), "pw_Retl_123", "recorded headers hold no credentials")
			require.NotContains(t, r.Query.Encode(), "secret", "recorded query strings hold no credentials")
			require.NotZero(t, r.At)
		}
		require.Equal(t, reqs[0].ConnID, reqs[1].ConnID, "keep-alive reuses the connection")
		require.Positive(t, reqs[1].IdleBefore, "the second request on a connection measures the idle gap")
		require.Equal(t, "drop", reqs[2].Query.Get("query_id"))
	})
}
