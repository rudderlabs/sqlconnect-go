package sshtunnel

import (
	"fmt"
	"net"
	"strconv"

	"github.com/rudderlabs/sql-tunnels/tunnel"
)

// NewTcpTunnel creates a new ssh tunnel forwading tcp traffic
func NewTcpTunnel(c Config, remoteHost string, remotePort int) (Tunnel, error) {
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("invalid ssh tunnel configuration: %w", err)
	}
	port, _ := strconv.Atoi(c.Port)

	tunnelConfig := tunnel.SSHConfig{
		User:       c.User,
		Host:       c.Host,
		Port:       port,
		PrivateKey: []byte(c.PrivateKey),

		RemoteHost: remoteHost,
		RemotePort: remotePort,
	}
	t, err := tunnel.ListenAndForward(&tunnelConfig)
	if err != nil {
		return nil, fmt.Errorf("creating ssh tunnel: %w", err)
	}
	return &tcpTunnel{t}, nil
}

type tcpTunnel struct {
	*tunnel.SSH
}

func (t *tcpTunnel) Host() string {
	host, _, _ := net.SplitHostPort(t.Addr())
	return host
}

func (t *tcpTunnel) Port() int {
	_, port, _ := net.SplitHostPort(t.Addr())
	p, _ := strconv.Atoi(port)
	return p
}
