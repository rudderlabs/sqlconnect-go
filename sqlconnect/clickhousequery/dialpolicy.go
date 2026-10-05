package clickhousequery

import (
	"fmt"
	"net/netip"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
)

// DialPolicy carries the operator address rules for every ClickHouse
// connection in the process.
type DialPolicy struct {
	// BlockedPrefixes adds refused ranges to the fixed refused address set.
	BlockedPrefixes []netip.Prefix
	// AllowLoopback admits loopback answers. Test builds only: production
	// code never sets it, because a loopback answer reaches the pod itself.
	AllowLoopback bool
	// AllowPlainHTTP lets the parser admit secure=false. Test builds only:
	// production code never sets it, because plain HTTP sends the password
	// in clear text.
	AllowPlainHTTP bool
}

// SetDialPolicy installs the operator policy once. rudder-sources calls it
// lazily from NewClient. Before installation, connections use the fixed
// refused address set without operator CIDRs or test switches. Installation
// affects subsequent dials, including those from existing pools, but does not
// close established connections. AllowPlainHTTP is checked when NewDB parses
// the config, so that test switch must be installed before NewDB. A malformed
// prefix or a second explicit call returns an error.
func SetDialPolicy(p DialPolicy) error {
	return chpolicy.Install(chpolicy.Policy{
		Blocked:        p.BlockedPrefixes,
		AllowLoopback:  p.AllowLoopback,
		AllowPlainHTTP: p.AllowPlainHTTP,
	})
}

// ParseBlockedCIDRs parses a comma-separated list of CIDR ranges. One bad
// entry fails the whole call. rudder-sources calls it once per entry of
// RSOURCES_SQLSOURCE_CLICKHOUSE_BLOCKEDCIDRS and skips a bad entry with a
// warning, so the block list never stops the service.
func ParseBlockedCIDRs(s string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for part := range strings.SplitSeq(s, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		p, err := netip.ParsePrefix(part)
		if err == nil {
			err = chpolicy.Validate(chpolicy.Policy{Blocked: []netip.Prefix{p}})
		}
		if err != nil {
			return nil, fmt.Errorf("clickhouse blocked CIDR %q is malformed", part)
		}
		out = append(out, p)
	}
	return out, nil
}
