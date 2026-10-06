package chpolicy_test

import (
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
)

func TestSQ26_BlockList(t *testing.T) {
	blocked := chpolicy.Policy{Blocked: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}}
	for addr, refused := range map[string]bool{
		"10.1.2.3": true, "192.168.1.1": false, "172.16.0.1": false,
		"::ffff:10.1.2.3": true, "64:ff9b::a01:203": true, "2002:a01:203::": true, // IPv4 and embedded forms
		"64:ff9b:1:a01:2:300::": true, "2001:0:a01:203::": true, "2001:0:808:808::f5fe:fdfc": true, // /48 NAT64, Teredo server and inverted client
	} {
		require.Equal(t, refused, blocked.RefusedReason(net.ParseIP(addr)) != "", addr)
	}
	require.Empty(t, chpolicy.Policy{}.RefusedReason(net.ParseIP("10.1.2.3")), "an empty list admits private ranges")
	require.Empty(t, chpolicy.Policy{}.RefusedReason(net.ParseIP("fd12:3456::1")), "an empty list admits ULA")
	v6 := chpolicy.Policy{Blocked: []netip.Prefix{netip.MustParsePrefix("fd00::/8")}}
	require.Equal(t, "operator block list", v6.RefusedReason(net.ParseIP("fd12:3456::1")))
	wide := chpolicy.Policy{Blocked: []netip.Prefix{netip.MustParsePrefix("0.0.0.0/0")}}
	require.NotEmpty(t, wide.RefusedReason(net.ParseIP("169.254.169.254")), "a listed range never admits a fixed-set address")
	require.NotEmpty(t, chpolicy.Policy{Blocked: []netip.Prefix{netip.MustParsePrefix("8.8.8.0/24")}}.RefusedReason(net.ParseIP("127.0.0.1")),
		"an unrelated list keeps the fixed set")
	require.Error(t, chpolicy.Validate(chpolicy.Policy{Blocked: []netip.Prefix{netip.MustParsePrefix("10.1.2.3/8")}}))
	require.Error(t, chpolicy.Validate(chpolicy.Policy{Blocked: []netip.Prefix{{}}}))
	require.NoError(t, chpolicy.Validate(chpolicy.Policy{Blocked: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("fd00::/8")}}))
}

// An IPv4-mapped prefix never matches, because answers are checked in their
// IPv4 form. Validate refuses it so an operator entry is never a silent no-op.
func TestSQ26_MappedPrefixRefused(t *testing.T) {
	require.Error(t, chpolicy.Validate(chpolicy.Policy{Blocked: []netip.Prefix{netip.MustParsePrefix("::ffff:10.0.0.0/104")}}))
}

func TestSQ26_UnparseableAddressRefused(t *testing.T) {
	require.NotEmpty(t, chpolicy.Policy{}.RefusedReason(nil))
	require.NotEmpty(t, chpolicy.Policy{AllowLoopback: true}.RefusedReason(net.IP{1, 2, 3}))
}

func TestSQ26_AllowLoopbackRelaxesOnlyLoopback(t *testing.T) {
	p := chpolicy.Policy{AllowLoopback: true}
	require.Empty(t, p.RefusedReason(net.ParseIP("127.0.0.1")))
	require.Empty(t, p.RefusedReason(net.ParseIP("::1")))
	require.NotEmpty(t, p.RefusedReason(net.ParseIP("169.254.169.254")))
	require.NotEmpty(t, p.RefusedReason(net.ParseIP("0.0.0.0")))
	require.NotEmpty(t, p.RefusedReason(net.ParseIP("64:ff9b::7f00:1")), "embedded loopback stays refused")
	require.NotEmpty(t, chpolicy.Policy{}.RefusedReason(net.ParseIP("127.0.0.1")))
	blocked := chpolicy.Policy{AllowLoopback: true, Blocked: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}}
	require.Equal(t, "operator block list", blocked.RefusedReason(net.ParseIP("127.0.0.1")), "the operator list wins")
}

func TestSQ26_CurrentBeforeInstall(t *testing.T) {
	_, ok := chpolicy.Current()
	require.False(t, ok, "no policy until Install runs")
}
