package chtest_test

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
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

func TestFixture_ProxyResetModes(t *testing.T) {
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	for _, plain := range []bool{true, false} {
		p := chtest.NewProxy(t, srv, chtest.ProxyOptions{PlainHTTP: plain})
		scheme := map[bool]string{true: "http", false: "https"}[plain]
		c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: srv.CA}}}
		post := func(body string) error {
			resp, err := c.Post(fmt.Sprintf("%s://localhost:%d/?query_id=q1", scheme, p.Port()), "text/plain", strings.NewReader(body))
			if err == nil {
				_, err = io.ReadAll(resp.Body)
				_ = resp.Body.Close()
			}
			return err
		}
		p.ResetAfterForwardOnce(func(chtest.Request) bool { return true })
		require.ErrorIs(t, post("SELECT 1"), syscall.ECONNRESET, "%s: reset after the server answered", scheme)
		require.NoError(t, post("SELECT 1"), "one-shot rules fire once")
		p.ResetBeforeBodyOnce(func(r chtest.Request) bool { return r.Query.Get("query_id") == "q1" })
		err := post("SELECT 1 -- " + strings.Repeat("x", 4<<20))
		require.True(t, errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE), "%s: observed %v", scheme, err)
		t.Logf("%s reset-before-body observed: %v", scheme, err)
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
