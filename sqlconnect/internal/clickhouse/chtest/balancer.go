package chtest

import (
	"crypto/tls"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// Balancer is a TCP front that alternates upstreams per connection, as a load
// balancer in front of a cluster does. It ends TLS with the server's
// certificate, so the client trusts it with that server's CA, and relays the
// plain bytes to each upstream port on 127.0.0.1. An upstream is a plain HTTP
// port: a server's HTTPPort or a PlainHTTP proxy, which lets a test fake a
// second host with one container.
type Balancer struct {
	ln net.Listener
}

// NewBalancer starts a balancer on 127.0.0.1 that ends TLS with srv's
// certificate and alternates the upstream ports.
func NewBalancer(t *testing.T, srv *Server, upstreams ...int) *Balancer {
	t.Helper()
	require.NotEmpty(t, upstreams)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{srv.serverCert}, MinVersion: tls.VersionTLS12,
	})
	require.NoError(t, err)
	b := &Balancer{ln: ln}
	var next atomic.Int64
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			port := upstreams[int(next.Add(1)-1)%len(upstreams)]
			wg.Go(func() { relay(c, net.JoinHostPort("127.0.0.1", strconv.Itoa(port))) })
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return b
}

// Port returns the balancer's port on 127.0.0.1.
func (b *Balancer) Port() int { return b.ln.Addr().(*net.TCPAddr).Port }

func relay(c net.Conn, upstream string) {
	defer func() { _ = c.Close() }()
	up, err := net.Dial("tcp", upstream)
	if err != nil {
		return
	}
	defer func() { _ = up.Close() }()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, c); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
	<-done
}
