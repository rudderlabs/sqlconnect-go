package driver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSetHTTPClient(t *testing.T) {
	var used atomic.Bool
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		used.Store(true)
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: r}, nil
	})}
	rc := restclient.NewOdpsRestClient(account.NewAliyunAccount("id", "secret", "cn-test"), "http://tunnel.invalid")
	require.NoError(t, setHTTPClient(&rc, hc), "the pinned SDK keeps its http.Client in RestClient._client")
	req, err := rc.NewRequest(http.MethodGet, "/projects/p", nil)
	require.NoError(t, err)
	resp, err := rc.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.True(t, used.Load(), "the rest client sent the request through the injected client")
}

// A tunnel server that accepts the session create and never answers must
// not hold the request open: the idle deadline fails it, the create is
// retried, and openRows returns a network error with every connection closed.
func TestTunnelSessionCreateIdle(t *testing.T) {
	var open, started atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Add(1)
		open.Add(1)
		defer open.Add(-1)
		select {
		case <-r.Context().Done(): // the client closed the connection
		case <-time.After(8 * time.Second): // bounded, so a regression fails instead of hanging srv.Close
		}
	}))
	t.Cleanup(srv.Close)

	o := odps.NewOdps(account.NewAliyunAccount("id", "secret", "cn-test"), srv.URL)
	o.SetDefaultProjectName("p")
	cfg := Config{Client: o, TunnelIdle: 200 * time.Millisecond}
	cfg.applyDefaults()
	c := &Connector{cfg: cfg}
	require.NoError(t, c.setTunnel(srv.URL))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	_, err := c.openRows(ctx, o.Instance("inst1"))
	var e *mcerr.Error
	require.ErrorAs(t, err, &e)
	require.Equal(t, mcerr.CategoryNetwork, e.Category, err.Error())
	require.Less(t, time.Since(start), 5*time.Second, "the idle deadline, not the caller's context, ended the create")
	require.EqualValues(t, 1+transportRetries, started.Load(), "the create is retried after an idle timeout")
	require.Eventually(t, func() bool { return open.Load() == 0 }, 2*time.Second, 10*time.Millisecond, "no request outlives openRows")
}

// A result stream that goes silent fails within the idle deadline with a
// transport error, which readRange resumes; a slow but steady stream that
// lasts longer than the deadline is not cut.
func TestIdleHTTPClientStream(t *testing.T) {
	const idle = 200 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		for range 8 {
			_, _ = w.Write([]byte("x"))
			w.(http.Flusher).Flush()
			time.Sleep(idle / 4)
		}
		if r.URL.Path == "/stall" {
			<-r.Context().Done()
		}
	}))
	t.Cleanup(srv.Close)
	hc := idleHTTPClient(idle, time.Second)

	res, err := hc.Get(srv.URL + "/steady")
	require.NoError(t, err)
	body, err := io.ReadAll(res.Body)
	_ = res.Body.Close()
	require.NoError(t, err, "a stream that keeps sending is never cut, though it lasts 2x idle")
	require.Equal(t, "xxxxxxxx", string(body))

	res, err = hc.Get(srv.URL + "/stall")
	require.NoError(t, err)
	start := time.Now()
	body, err = io.ReadAll(res.Body)
	_ = res.Body.Close()
	require.Error(t, err)
	require.Equal(t, "xxxxxxxx", string(body))
	require.Less(t, time.Since(start), 2*time.Second)
	require.True(t, mcerr.IsTransport(err), "a stalled read is a transport error: %v", err)
	require.True(t, resumable(err), "readRange resumes a stalled range")
	require.False(t, errors.Is(err, context.Canceled))
}

// The Instance Tunnel caps a result at 10,000 rows when a session is opened
// with EnableLimit. The driver never sets it: no tunnel request carries
// instance_tunnel_limit_enabled, and a 25,000-row result is reported and
// requested whole.
func TestTunnelSessionHasNoRowLimit(t *testing.T) {
	const total = 25_000
	var mu sync.Mutex
	var queries []url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries = append(queries, r.URL.Query())
		mu.Unlock()
		if r.Method == http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"DownloadID":"d1","Status":"normal","RecordCount":%d,"Schema":{"columns":[{"name":"id","type":"bigint","nullable":true}]}}`, total)
			return
		}
		w.WriteHeader(http.StatusNotFound) // no range body is needed
	}))
	t.Cleanup(srv.Close)

	o := odps.NewOdps(account.NewAliyunAccount("id", "secret", "cn-test"), srv.URL)
	o.SetDefaultProjectName("p")
	cfg := Config{Client: o, TunnelIdle: time.Second}
	cfg.applyDefaults()
	c := &Connector{cfg: cfg}
	require.NoError(t, c.setTunnel(srv.URL))

	var reported int64
	ctx := WithRowCountHandler(context.Background(), func(n int64) { reported = n })
	r, err := c.openRows(ctx, o.Instance("inst1"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	require.EqualValues(t, total, reported, "the session reports the whole result")

	ranges := func() (create int, rowranges []string, limited []url.Values) {
		mu.Lock()
		defer mu.Unlock()
		for _, q := range queries {
			if q.Has("instance_tunnel_limit_enabled") {
				limited = append(limited, q)
			}
			if q.Has("downloads") {
				create++
			}
			if q.Has("rowrange") {
				rowranges = append(rowranges, q.Get("rowrange"))
			}
		}
		return create, rowranges, limited
	}
	require.Eventually(t, func() bool { _, rr, _ := ranges(); return len(rr) > 0 }, 3*time.Second, 10*time.Millisecond)
	create, rowranges, limited := ranges()
	require.Empty(t, limited, "a tunnel request enabled the 10,000-row limit")
	require.Equal(t, 1, create)
	require.Equal(t, fmt.Sprintf("(0,%d)", total), rowranges[0], "the whole result is requested, past 10,000 rows")
}

// With the SDK's own reader: a range whose deflate body stalls inside the
// zlib stream holds a Read in the decoder. Close must cancel the range
// request (the server sees it end) and return promptly; under -race, a
// Close of the decoder during that Read would be reported.
func TestCloseCancelsARangeReadInFlight(t *testing.T) {
	var reading atomic.Bool
	var cancelled atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"DownloadID":"d1","Status":"normal","RecordCount":10,"Schema":{"columns":[{"name":"id","type":"bigint","nullable":true}]}}`)
			return
		}
		w.Header().Set("Content-Encoding", "deflate")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte{0x78, 0x9c}) // a zlib header, then nothing
		w.(http.Flusher).Flush()
		reading.Store(true)
		select {
		case <-r.Context().Done():
			cancelled.Store(true)
		case <-time.After(8 * time.Second):
		}
	}))
	t.Cleanup(srv.Close)

	o := odps.NewOdps(account.NewAliyunAccount("id", "secret", "cn-test"), srv.URL)
	o.SetDefaultProjectName("p")
	cfg := Config{Client: o, TunnelIdle: 30 * time.Second}
	cfg.applyDefaults()
	c := &Connector{cfg: cfg}
	require.NoError(t, c.setTunnel(srv.URL))

	r, err := c.openRows(context.Background(), o.Instance("inst1"))
	require.NoError(t, err)
	require.Eventually(t, reading.Load, 3*time.Second, time.Millisecond)
	time.Sleep(100 * time.Millisecond) // the reader is now blocked in the decoder
	start := time.Now()
	require.NoError(t, r.Close())
	require.Less(t, time.Since(start), time.Second, "Close cancels the request rather than waiting on the stalled body")
	require.Eventually(t, cancelled.Load, 2*time.Second, 10*time.Millisecond, "the range request was cancelled")
}
