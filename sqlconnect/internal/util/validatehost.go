package util

import (
	"fmt"
	"net"
)

// HostValidationOption customises ValidateHost.
type HostValidationOption func(*hostValidationOptions)

type hostValidationOptions struct {
	allowLoopback bool
}

// AllowLoopback permits hosts that resolve to loopback addresses when allow is
// true, and is a no-op otherwise, so callers can pass a flag through directly.
//
// Intended only for tests that run against a database in a local container.
// It deliberately relaxes nothing else: link-local, private and unspecified
// addresses stay rejected, so this cannot be used to reach cloud metadata or
// in-cluster services.
func AllowLoopback(allow bool) HostValidationOption {
	return func(o *hostValidationOptions) { o.allowLoopback = allow }
}

// ValidateHost checks that the hostname is resolvable and that none of the
// addresses it resolves to are ones a warehouse connection should ever reach.
//
// Rejected: unspecified (0.0.0.0, ::), loopback, link-local — which covers the
// cloud instance metadata endpoint 169.254.169.254 — multicast, and private
// RFC1918/ULA space, which is how in-cluster services are addressed.
//
// Note this cannot defend against DNS rebinding: the address checked here is
// not necessarily the address dialled later. It raises the bar for a
// caller-supplied host without claiming to close that gap.
func ValidateHost(hostname string, opts ...HostValidationOption) error {
	var options hostValidationOptions
	for _, opt := range opts {
		opt(&options)
	}

	addrs, err := net.LookupHost(hostname)
	if err != nil {
		return fmt.Errorf("looking up hostname %s: %w", hostname, err)
	}

	for _, addr := range addrs {
		ip := net.ParseIP(addr)
		if ip == nil {
			return fmt.Errorf("invalid host in credentials: %s resolves to an unparseable address", hostname)
		}
		if ip.IsLoopback() && options.allowLoopback {
			continue
		}
		if reason := disallowedAddrReason(ip); reason != "" {
			return fmt.Errorf("invalid host in credentials: %s resolves to a %s address", hostname, reason)
		}
	}
	return nil
}

func disallowedAddrReason(ip net.IP) string {
	switch {
	case ip.IsUnspecified():
		return "unspecified"
	case ip.IsLoopback():
		return "loopback"
	case ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast():
		return "link-local"
	case ip.IsInterfaceLocalMulticast(), ip.IsMulticast():
		return "multicast"
	case ip.IsPrivate():
		return "private"
	default:
		return ""
	}
}
