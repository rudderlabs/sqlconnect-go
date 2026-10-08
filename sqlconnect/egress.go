package sqlconnect

import (
	"fmt"
	"net"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

// SetEgressPolicy configures a process-wide list of CIDR ranges that warehouse
// connections must never reach (for example in-cluster service ranges), checked
// at dial time against the actual resolved address in addition to the always-
// blocked classes (loopback, link-local, metadata, ...). It is set once at
// startup and applies to every connection the process opens. An empty or nil
// list clears it. Ranges are parsed with net.ParseCIDR; an invalid entry returns
// an error and the policy is left unchanged.
func SetEgressPolicy(blockedCIDRs []string) error {
	parsed := make([]*net.IPNet, 0, len(blockedCIDRs))
	for _, s := range blockedCIDRs {
		_, cidr, err := net.ParseCIDR(s)
		if err != nil {
			return fmt.Errorf("invalid CIDR %q: %w", s, err)
		}
		parsed = append(parsed, cidr)
	}
	util.SetBlockedCIDRs(parsed)
	return nil
}
