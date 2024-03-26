package sshtunnel

// Tunnel is an ssh tunnel
type Tunnel interface {
	// Addr returns the address that the tunnel is listening on
	Addr() string
	// Host returns the host that the tunnel is listening on
	Host() string
	// Port returns the port that the tunnel is listening on
	Port() int
	// Close closes the tunnel
	Close() error
}
