package sshtunnel

// NoTunnelCloser is a function that does nothing and returns nil.
var NoTunnelCloser func() error = func() error { return nil }
