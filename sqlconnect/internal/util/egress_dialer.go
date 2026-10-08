package util

import (
	"context"
	"net"
	"net/http"
	"time"
)

// GuardedHTTPTransport returns a clone of http.DefaultTransport whose DialContext
// is guarded by the egress policy. Use it for the HTTPS-based connectors
// (snowflake, trino, databricks, bigquery), so the TCP connection underneath each
// HTTPS request is checked against the dialled IP.
func GuardedHTTPTransport(allowLoopback bool) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = GuardedDialContext(allowLoopback)
	return t
}

// GuardedDialer enforces the egress policy and satisfies lib/pq's Dialer and
// DialerContext interfaces, for the connectors that accept a pq dialer.
type GuardedDialer struct {
	dial func(context.Context, string, string) (net.Conn, error)
}

// NewGuardedDialer returns a GuardedDialer bound to the current egress policy.
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
