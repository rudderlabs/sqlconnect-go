package clickhousequery_test

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
)

// The policy is process-wide and installs once, so this is the only test in
// the package that calls SetDialPolicy.
func TestSQ26_SetDialPolicyOnce(t *testing.T) {
	_, err := clickhousequery.ParseBlockedCIDRs("10.0.0.0/8,not-a-cidr")
	require.Error(t, err)
	_, err = clickhousequery.ParseBlockedCIDRs("10.1.2.3/8") // host bits set
	require.Error(t, err)
	_, err = clickhousequery.ParseBlockedCIDRs("::ffff:10.0.0.0/104") // never matches an IPv4 answer
	require.Error(t, err)
	require.Error(t, clickhousequery.SetDialPolicy(clickhousequery.DialPolicy{BlockedPrefixes: []netip.Prefix{{}}}))
	_, ok := chpolicy.Current()
	require.False(t, ok, "a refused policy installs nothing")

	prefixes, err := clickhousequery.ParseBlockedCIDRs(" 10.0.0.0/8 , fd00::/8 ")
	require.NoError(t, err)
	require.Equal(t, []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("fd00::/8")}, prefixes)
	require.NoError(t, clickhousequery.SetDialPolicy(clickhousequery.DialPolicy{BlockedPrefixes: prefixes, AllowPlainHTTP: true}))
	prefixes[0] = netip.MustParsePrefix("192.168.0.0/16") // the caller's slice no longer affects the policy

	got, ok := chpolicy.Current()
	require.True(t, ok)
	require.Equal(t, []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("fd00::/8")}, got.Blocked)
	require.True(t, got.AllowPlainHTTP)
	require.False(t, got.AllowLoopback)
	got.Blocked[0] = netip.MustParsePrefix("192.168.0.0/16")
	again, _ := chpolicy.Current()
	require.Equal(t, netip.MustParsePrefix("10.0.0.0/8"), again.Blocked[0], "Current returns a copy")

	require.Error(t, clickhousequery.SetDialPolicy(clickhousequery.DialPolicy{}), "a second call fails")

	empty, err := clickhousequery.ParseBlockedCIDRs("")
	require.NoError(t, err)
	require.Empty(t, empty)
	empty, err = clickhousequery.ParseBlockedCIDRs(" , ")
	require.NoError(t, err)
	require.Empty(t, empty)
}
