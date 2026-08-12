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
// It deliberately relaxes nothing else: link-local, multicast and unspecified
// addresses stay rejected, so this cannot be used to reach cloud metadata.
func AllowLoopback(allow bool) HostValidationOption {
	return func(o *hostValidationOptions) { o.allowLoopback = allow }
}

// ValidateHost checks that the hostname is resolvable and that none of the
// addresses it resolves to are ones a warehouse connection should ever reach.
//
// Rejected: unspecified (0.0.0.0, ::), loopback, link-local — which covers the
// instance metadata endpoint 169.254.169.254 — multicast, and AWS's reserved
// prefix for instance metadata over IPv6.
//
// Private RFC1918/ULA space is deliberately NOT rejected. Warehouses reached
// over AWS PrivateLink resolve to a private address in the customer's VPC, so
// rejecting private space would break every such connection. Those ranges are
// customer-chosen and can overlap our own, which means they cannot be told
// apart from in-cluster addresses by inspecting the IP alone — blocking
// in-cluster services needs an operator-supplied CIDR list instead, which is
// tracked separately.
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
	case awsIMDSv6.Contains(ip):
		return "instance metadata"
	default:
		return ""
	}
}

// awsIMDSv6 is the prefix AWS reserves for the instance metadata service over
// IPv6, where the endpoint is fd00:ec2::254.
//
// It sits inside fc00::/7, so it used to be caught by the blanket rejection of
// private space. That rejection had to go for PrivateLink, and unlike the IPv4
// endpoint this one is not link-local, so nothing else catches it. Blocking the
// reserved prefix is safe: it belongs to AWS, so no customer VPC is numbered
// from it.
var awsIMDSv6 = &net.IPNet{IP: net.ParseIP("fd00:ec2::"), Mask: net.CIDRMask(32, 128)}
