package clickhouse

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

// maxRetryAfter caps the server's Retry-After hint, so a hostile or broken
// server cannot park a sync for hours.
const maxRetryAfter = 5 * time.Minute

// guardedTransport sits between the fork's HTTP client and the pinned
// transport. It returns an error for every 3xx answer, so neither the fork nor
// http.Client ever follows a redirect to a second host, and it turns HTTP 429
// into CH_RATE_LIMITED with a bounded Retry-After.
type guardedTransport struct {
	next *http.Transport
	now  func() time.Time
}

// newTransportFunc returns the value for clickhouse.Options.TransportFunc.
// The environment proxy is removed because a proxy would dial an address the
// guarded dialer never checked.
func newTransportFunc() func(*http.Transport) (http.RoundTripper, error) {
	return func(rt *http.Transport) (http.RoundTripper, error) {
		rt.Proxy = nil
		rt.IdleConnTimeout = 5 * time.Second
		rt.TLSHandshakeTimeout = 90 * time.Second
		return &guardedTransport{next: rt, now: time.Now}, nil
	}
}

func (g *guardedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := g.next.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		drainClose(resp.Body)
		return nil, cherr.New(cherr.CodeRedirectRefused, "host", fixedMessages[cherr.CodeRedirectRefused])
	case resp.StatusCode == http.StatusTooManyRequests:
		e := cherr.New(cherr.CodeRateLimited, "", fixedMessages[cherr.CodeRateLimited])
		e.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), g.now())
		drainClose(resp.Body)
		return nil, e
	}
	return resp, nil
}

// CloseIdleConnections lets http.Client.CloseIdleConnections, which the fork
// calls on Close, reach the wrapped transport.
func (g *guardedTransport) CloseIdleConnections() { g.next.CloseIdleConnections() }

// drainClose reads a bounded prefix so a small body can return the connection
// to the pool, and never reads an unbounded body from the server.
func drainClose(b io.ReadCloser) {
	_, _ = io.CopyN(io.Discard, b, 4096)
	_ = b.Close()
}

// parseRetryAfter reads delay-seconds or an HTTP date. A missing, negative,
// past or malformed value gives zero; every result is at most maxRetryAfter.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	var d time.Duration
	if v != "" && strings.Trim(v, "0123456789") == "" {
		if s, err := strconv.ParseInt(v, 10, 64); err == nil {
			d = time.Duration(min(s, int64(maxRetryAfter/time.Second))) * time.Second
		} else if errors.Is(err, strconv.ErrRange) {
			d = maxRetryAfter
		}
	} else if t, err := http.ParseTime(v); err == nil && t.After(now) {
		d = t.Sub(now)
	}
	return min(d, maxRetryAfter)
}
