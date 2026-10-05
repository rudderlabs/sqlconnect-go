package clickhouse

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
)

func TestSQ30_RedirectRefusedUnit(t *testing.T) {
	var second atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { second.Add(1) }))
	defer other.Close()
	for name, loc := range map[string]string{"same origin": "/other", "cross origin": other.URL + "/", "https to http": other.URL + "/"} {
		for _, status := range []int{301, 302, 303, 307, 308} {
			t.Run(fmt.Sprintf("%s %d", name, status), func(t *testing.T) {
				var first atomic.Int32
				srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					first.Add(1)
					http.Redirect(w, r, loc, status)
				}))
				defer srv.Close()
				inner := srv.Client().Transport.(*http.Transport).Clone()
				rt, err := newTransportFunc()(inner)
				require.NoError(t, err)
				resp, err := (&http.Client{Transport: rt}).Post(srv.URL, "text/plain", strings.NewReader("SELECT 1"))
				if resp != nil {
					_ = resp.Body.Close()
				}
				require.Nil(t, resp)
				d, _ := clickhousequery.Describe(err)
				require.Equal(t, "CH_REDIRECT_REFUSED", d.Code)
				require.Equal(t, "host", d.Field)
				require.EqualValues(t, 1, first.Load(), "the redirect target is never requested, not even on the same origin")
			})
		}
	}
	require.Zero(t, second.Load(), "the second host receives zero requests")
}

func TestSQ22_RateLimited(t *testing.T) {
	for header, want := range map[string]time.Duration{
		"30": 30 * time.Second, "": 0, "100000000": 5 * time.Minute, "-3": 0, " 7 ": 7 * time.Second,
		"99999999999999999999999": 5 * time.Minute, "99999999999999999999999garbage": 0, "+7": 0, "soon": 0,
		time.Unix(1_000_060, 0).UTC().Format(http.TimeFormat): 60 * time.Second,
		time.Unix(999_000, 0).UTC().Format(http.TimeFormat):   0,
		time.Unix(9_000_000, 0).UTC().Format(http.TimeFormat): 5 * time.Minute,
	} {
		t.Run(fmt.Sprintf("%q", header), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if header != "" {
					w.Header().Set("Retry-After", header)
				}
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte("Code: 202. DB::Exception: too many simultaneous queries"))
			}))
			defer srv.Close()
			g := &guardedTransport{next: http.DefaultTransport.(*http.Transport).Clone(), now: func() time.Time { return time.Unix(1_000_000, 0) }}
			defer g.CloseIdleConnections()
			resp, err := (&http.Client{Transport: g}).Get(srv.URL)
			if resp != nil {
				_ = resp.Body.Close()
			}
			require.Nil(t, resp)
			d, _ := clickhousequery.Describe(err)
			require.Equal(t, "CH_RATE_LIMITED", d.Code)
			require.Equal(t, "transient", d.Category)
			require.Equal(t, want, d.RetryAfter)
			require.NotContains(t, err.Error(), "DB::Exception", "the response body never reaches the error text")
		})
	}
}

func TestTransportRejectsStalledBodies(t *testing.T) {
	for _, tc := range []struct {
		status  int
		code    string
		message string
	}{
		{http.StatusFound, "CH_REDIRECT_REFUSED", "the server sent a redirect, which is refused"},
		{http.StatusTooManyRequests, "CH_RATE_LIMITED", "the server limited the request rate"},
	} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			closed := make(chan struct{})
			go func() {
				defer server.Close()
				if _, err := http.ReadRequest(bufio.NewReader(server)); err != nil {
					return
				}
				if _, err := fmt.Fprintf(server, "HTTP/1.1 %d %s\r\nContent-Length: 10000\r\n\r\n", tc.status, http.StatusText(tc.status)); err != nil {
					return
				}
				var b [1]byte
				_, _ = server.Read(b[:])
				close(closed)
			}()
			inner := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) { return client, nil }}
			rt, err := newTransportFunc()(inner)
			require.NoError(t, err)
			defer rt.(*guardedTransport).CloseIdleConnections()
			result := make(chan error, 1)
			go func() {
				resp, err := (&http.Client{Transport: rt}).Get("http://clickhouse.test/")
				if resp != nil {
					_ = resp.Body.Close()
				}
				result <- err
			}()
			select {
			case err := <-result:
				d, _ := clickhousequery.Describe(err)
				require.Equal(t, tc.code, d.Code)
				require.ErrorContains(t, err, tc.message)
			case <-time.After(time.Second):
				t.Fatal("status response blocked on its stalled body")
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("status response body was not closed")
			}
		})
	}
}

func TestTransportPassesOtherStatuses(t *testing.T) {
	for _, status := range []int{200, 400, 401, 404, 500, 503} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte("body"))
		}))
		rt, err := newTransportFunc()(http.DefaultTransport.(*http.Transport).Clone())
		require.NoError(t, err)
		resp, err := (&http.Client{Transport: rt}).Get(srv.URL)
		require.NoError(t, err, "status %d reaches the driver's own error handling", status)
		require.Equal(t, status, resp.StatusCode)
		_ = resp.Body.Close()
		rt.(*guardedTransport).CloseIdleConnections()
		srv.Close()
	}
}

func TestTransportOptions(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://proxy.invalid:3128")
	t.Setenv("HTTP_PROXY", "http://proxy.invalid:3128")
	inner := &http.Transport{Proxy: http.ProxyFromEnvironment}
	rt, err := newTransportFunc()(inner)
	require.NoError(t, err)
	require.Nil(t, inner.Proxy)
	require.Equal(t, 5*time.Second, inner.IdleConnTimeout)
	require.Equal(t, 90*time.Second, inner.TLSHandshakeTimeout)
	g := rt.(*guardedTransport)
	require.Same(t, inner, g.next)
}

func TestCloseIdleConnectionsForwarded(t *testing.T) {
	var closed atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateClosed {
			closed.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()
	inner := &http.Transport{}
	rt, err := newTransportFunc()(inner)
	require.NoError(t, err)
	inner.IdleConnTimeout = time.Hour // only the forwarded close can end the connection within the window
	c := &http.Client{Transport: rt}
	resp, err := c.Get(srv.URL)
	require.NoError(t, err)
	_ = resp.Body.Close()
	c.CloseIdleConnections()
	require.Eventually(t, func() bool { return closed.Load() == 1 }, 2*time.Second, 10*time.Millisecond)
}
