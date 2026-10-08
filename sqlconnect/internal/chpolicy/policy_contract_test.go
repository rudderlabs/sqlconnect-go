package chpolicy

import (
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

// Install is process-wide. Restore the initial state so -count and the existing
// external CurrentBeforeInstall test remain order independent. No parallel tests.
func TestPolicyInstallOwnsItsSnapshot(t *testing.T) {
	mu.Lock()
	old, wasSet := current, set
	current, set = Policy{}, false
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		current, set = old, wasSet
	})
	require.Error(t, Install(Policy{Blocked: []netip.Prefix{netip.MustParsePrefix("10.1.2.3/8")}}))
	_, installed := Current()
	require.False(t, installed, "invalid input must not consume the one installation")
	input := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	require.NoError(t, Install(Policy{Blocked: input, AllowLoopback: true, AllowPlainHTTP: true}))
	input[0] = netip.MustParsePrefix("8.8.8.0/24")
	first, installed := Current()
	require.True(t, installed)
	require.True(t, first.AllowLoopback)
	require.True(t, first.AllowPlainHTTP)
	require.Equal(t, "operator block list", first.RefusedReason(net.ParseIP("10.2.3.4")))
	first.Blocked[0] = netip.MustParsePrefix("1.1.1.0/24")
	require.Error(t, Install(Policy{}), "a second caller cannot erase operator policy")
	second, installed := Current()
	require.True(t, installed)
	require.Equal(t, []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}, second.Blocked)
	require.True(t, second.AllowLoopback)
	require.True(t, second.AllowPlainHTTP)
}

func FuzzPolicyBlocksMappedIPv4(f *testing.F) {
	f.Add(byte(10), byte(1), byte(2), byte(3))
	f.Add(byte(127), byte(0), byte(0), byte(1))
	f.Fuzz(func(t *testing.T, a, b, c, d byte) {
		addr := netip.AddrFrom4([4]byte{a, b, c, d})
		p := Policy{Blocked: []netip.Prefix{netip.PrefixFrom(addr, 32)}, AllowLoopback: true}
		require.NoError(t, Validate(p))
		require.Equal(t, "operator block list", p.RefusedReason(net.IP{a, b, c, d}))
		require.Equal(t, "operator block list", p.RefusedReason(net.IPv4(a, b, c, d)))
	})
}
