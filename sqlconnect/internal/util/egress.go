package util

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"syscall"
	"time"
)

var blockedCIDRPolicy atomic.Pointer[[]*net.IPNet]

// SetBlockedCIDRs replaces the process-wide blocked ranges. It copies the
// ranges so callers cannot change the active policy after setting it.
func SetBlockedCIDRs(cidrs []*net.IPNet) {
	copyOfCIDRs := make([]*net.IPNet, len(cidrs))
	for i, cidr := range cidrs {
		if cidr != nil {
			copyOfCIDRs[i] = &net.IPNet{
				IP:   append(net.IP(nil), cidr.IP...),
				Mask: append(net.IPMask(nil), cidr.Mask...),
			}
		}
	}
	blockedCIDRPolicy.Store(&copyOfCIDRs)
}

func blockedCIDRs() []*net.IPNet {
	if cidrs := blockedCIDRPolicy.Load(); cidrs != nil {
		return *cidrs
	}
	return nil
}

// BlockedHostError reports an address rejected by the egress guard.
type BlockedHostError struct {
	IP     net.IP
	Reason string
}

func (e *BlockedHostError) Error() string {
	return fmt.Sprintf("connection to %s blocked: %s", e.IP, e.Reason)
}

func blockedReason(ip net.IP, allowLoopback bool) string {
	if ip.IsLoopback() && allowLoopback {
		return ""
	}
	if reason := disallowedAddrReason(ip); reason != "" {
		return reason
	}
	for _, cidr := range blockedCIDRs() {
		if cidr != nil && cidr.Contains(ip) {
			return "blocked range"
		}
	}
	return ""
}

// GuardedDialContext returns a dial function that checks each resolved address
// against the process-wide egress policy before connecting.
func GuardedDialContext(allowLoopback bool) func(ctx context.Context, network, addr string) (net.Conn, error) {
	ctrl := func(_ string, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return fmt.Errorf("dial address %q is not an IP address", host)
		}
		if reason := blockedReason(ip, allowLoopback); reason != "" {
			return &BlockedHostError{IP: ip, Reason: reason}
		}
		return nil
	}

	// Control runs after name resolution for every candidate address, so it
	// validates the dialled IP rather than the pre-resolved hostname. This
	// closes the resolve-then-dial (DNS rebinding) gap.
	d := &net.Dialer{Timeout: 30 * time.Second, Control: ctrl}
	return d.DialContext
}
