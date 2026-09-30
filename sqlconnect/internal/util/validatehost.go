package util

import (
	"bytes"
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
// prefix for instance metadata over IPv6. An IPv6 answer that embeds an IPv4
// address (NAT64, 6to4, Teredo) is also checked on the embedded address.
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
		if reason := DisallowedAddrReason(ip); reason != "" {
			return fmt.Errorf("invalid host in credentials: %s resolves to a %s address", hostname, reason)
		}
	}
	return nil
}

// DisallowedAddrReason returns why a connection must not reach ip, or "" when
// the address is allowed. It checks ip itself, then every IPv4 address that ip
// embeds (see EmbeddedIPv4), so a NAT64, 6to4 or Teredo form cannot hide a
// refused IPv4 address.
func DisallowedAddrReason(ip net.IP) string {
	if r := classReason(ip); r != "" {
		return r
	}
	for _, v4 := range EmbeddedIPv4(ip) {
		if r := classReason(v4); r != "" {
			return "embedded " + r
		}
	}
	return ""
}

// RefusedClass returns the shared-fixture class of ip and the embedded IPv4
// address the class was decided on. The class is "" when ip is allowed, and
// the embedded address is nil unless an embedded IPv4 address decided it.
func RefusedClass(ip net.IP) (string, net.IP) {
	if r := classReason(ip); r != "" {
		return classNames[r], nil
	}
	for _, v4 := range EmbeddedIPv4(ip) {
		if r := classReason(v4); r != "" {
			return classNames[r], v4.To4()
		}
	}
	return "", nil
}

// classNames maps a reason to its class in the shared address fixture
// (sqlconnect/clickhousequery/testdata/addresses.json). Lookout reads the same
// fixture, so these names are a cross-repository contract.
var classNames = map[string]string{
	"unspecified":       "unspecified",
	"loopback":          "loopback",
	"link-local":        "link_local",
	"multicast":         "multicast",
	"instance metadata": "metadata",
}

func classReason(ip net.IP) string {
	switch {
	case ip.IsUnspecified():
		return "unspecified"
	case ip.IsLoopback():
		return "loopback"
	// The refused address set puts all of 224.0.0.0/4 in the multicast class,
	// but Go reports 224.0.0.0/24 as link-local multicast. Only IPv6 ff02::/16
	// is link-local in the set.
	case ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast() && ip.To4() == nil:
		return "link-local"
	case ip.IsInterfaceLocalMulticast(), ip.IsMulticast():
		return "multicast"
	case awsIMDSv6.Contains(ip):
		return "instance metadata"
	default:
		return ""
	}
}

var (
	nat64WellKnown = mustCIDR("64:ff9b::/96")
	nat64Local     = mustCIDR("64:ff9b:1::/48")
	sixToFour      = mustCIDR("2002::/16")
	teredo         = mustCIDR("2001::/32")
)

func mustCIDR(s string) *net.IPNet {
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		panic(err)
	}
	return n
}

// EmbeddedIPv4 returns the IPv4 addresses that an IPv6 address carries under
// the NAT64 (64:ff9b::/96, 64:ff9b:1::/48), 6to4 (2002::/16) and Teredo
// (2001::/32) prefixes. IPv4 and ::ffff: forms return nil, because the class
// checks already read them as IPv4.
func EmbeddedIPv4(ip net.IP) []net.IP {
	b := ip.To16()
	if b == nil || ip.To4() != nil {
		return nil
	}
	switch {
	case nat64WellKnown.Contains(b):
		return []net.IP{net.IPv4(b[12], b[13], b[14], b[15])}
	case nat64Local.Contains(b):
		// RFC 6052: a /96 translator prefix leaves bytes 6-11 zero and puts the
		// address in bytes 12-15. The /48 layout uses bytes 6-7 and 9-10; byte 8
		// is the reserved "u" octet.
		if bytes.Equal(b[6:12], make([]byte, 6)) {
			return []net.IP{net.IPv4(b[12], b[13], b[14], b[15])}
		}
		return []net.IP{net.IPv4(b[6], b[7], b[9], b[10])}
	case sixToFour.Contains(b):
		return []net.IP{net.IPv4(b[2], b[3], b[4], b[5])}
	case teredo.Contains(b):
		// Teredo: the server is in bytes 4-7, the client in bytes 12-15 with
		// every bit inverted.
		return []net.IP{net.IPv4(b[4], b[5], b[6], b[7]), net.IPv4(^b[12], ^b[13], ^b[14], ^b[15])}
	default:
		return nil
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
