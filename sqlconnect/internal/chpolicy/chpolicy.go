// Package chpolicy stores the process-wide ClickHouse dial policy that
// clickhousequery.SetDialPolicy installs and the driver's dialer reads.
package chpolicy

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"sync"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

// Policy holds the operator block list and the test-only relaxations.
type Policy struct {
	Blocked        []netip.Prefix
	AllowLoopback  bool
	AllowPlainHTTP bool
}

var (
	mu      sync.Mutex
	current Policy
	set     bool
)

// Validate refuses an invalid prefix, a prefix with host bits set, and an
// IPv4-mapped prefix. Answers are checked in their IPv4 form, so a mapped
// prefix would never match and the operator entry would do nothing.
func Validate(p Policy) error {
	for _, pr := range p.Blocked {
		if !pr.IsValid() || pr != pr.Masked() || pr.Addr().Is4In6() {
			return fmt.Errorf("clickhouse dial policy: malformed prefix %q", pr.String())
		}
	}
	return nil
}

// Install validates p and makes it the process policy. It fails when a policy
// is already installed.
func Install(p Policy) error {
	if err := Validate(p); err != nil {
		return err
	}
	mu.Lock()
	defer mu.Unlock()
	if set {
		return errors.New("clickhouse dial policy is already set")
	}
	p.Blocked = slices.Clone(p.Blocked)
	current, set = p, true
	return nil
}

// Current returns a copy of the installed policy and whether one is installed.
func Current() (Policy, bool) {
	mu.Lock()
	defer mu.Unlock()
	p := current
	p.Blocked = slices.Clone(p.Blocked)
	return p, set
}

// RefusedReason returns "" when the policy admits ip. The operator list runs
// first, so AllowLoopback never admits a listed address. Each embedded IPv4
// address is checked against the list in its IPv4 form.
func (p Policy) RefusedReason(ip net.IP) string {
	if len(ip) != net.IPv4len && len(ip) != net.IPv6len {
		return "unparseable"
	}
	for _, c := range append([]net.IP{ip}, util.EmbeddedIPv4(ip)...) {
		a, ok := netip.AddrFromSlice(c)
		if !ok {
			continue
		}
		a = a.Unmap()
		for _, pr := range p.Blocked {
			if pr.Contains(a) {
				return "operator block list"
			}
		}
	}
	if p.AllowLoopback && ip.IsLoopback() {
		return ""
	}
	return util.DisallowedAddrReason(ip)
}
