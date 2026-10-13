package util

import (
	"context"
	"net"
	"net/http"
	"time"
)

// GuardedHTTPTransport returns a clone of http.DefaultTransport whose DialContext
// is guarded by the egress policy. Use it for the HTTPS-based connectors
// (snowflake, trino, databricks), so the TCP connection underneath each HTTPS
// request is checked against the dialled IP.
func GuardedHTTPTransport(allowLoopback bool) *http.Transport {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		// http.DefaultTransport is a *http.Transport in the standard library;
		// only a process that has replaced it with a different RoundTripper
		// reaches here. Fall back to a fresh transport rather than panic on the
		// type assertion.
		base = &http.Transport{}
	}
	t := base.Clone()
	t.DialContext = GuardedDialContext(allowLoopback)
	return t
}

// GuardedDialer enforces the egress policy and satisfies lib/pq's Dialer and
// DialerContext interfaces, for the connectors that accept a pq dialer.
type GuardedDialer struct {
	dial func(context.Context, string, string) (net.Conn, error)
}

// NewGuardedDialer returns a GuardedDialer that enforces the egress policy. The
// policy is re-read on every dial, so a later SetEgressPolicy takes effect
// without reopening the connection.
func NewGuardedDialer(allowLoopback bool) GuardedDialer {
	return GuardedDialer{dial: GuardedDialContext(allowLoopback)}
}

func (d GuardedDialer) Dial(network, address string) (net.Conn, error) {
	return d.dial(context.Background(), network, address)
}

func (d GuardedDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.dial(ctx, network, address)
}

func (d GuardedDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d.dial(ctx, network, address)
}
