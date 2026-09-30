package util_test

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

func TestValidateHost(t *testing.T) {
	t.Run("valid host", func(t *testing.T) {
		err := util.ValidateHost("github.com")
		require.NoError(t, err)
	})

	t.Run("invalid host", func(t *testing.T) {
		err := util.ValidateHost("!@#$.$%^")
		require.Error(t, err)
	})

	t.Run("localhost", func(t *testing.T) {
		err := util.ValidateHost("localhost")
		require.Error(t, err)
	})

	// Literal IPs below: net.LookupHost returns them as-is, so these assert on
	// address classification without depending on DNS.
	t.Run("rejects addresses a warehouse connection should never reach", func(t *testing.T) {
		for name, host := range map[string]string{
			"unspecified v4":     "0.0.0.0",
			"unspecified v6":     "::",
			"loopback v4":        "127.0.0.1",
			"loopback v4 subnet": "127.0.0.2",
			"loopback v6":        "::1",
			"instance metadata":  "169.254.169.254",
			"link-local v4":      "169.254.10.1",
			"link-local v6":      "fe80::1",
			"multicast":          "239.1.1.1",
			// Instance metadata over IPv6 sits in fc00::/7 and is not
			// link-local, so it needs its own guard now that private space is
			// allowed for PrivateLink.
			"instance metadata v6":        "fd00:ec2::254",
			"instance metadata v6 prefix": "fd00:ec2::1",
		} {
			t.Run(name, func(t *testing.T) {
				require.Error(t, util.ValidateHost(host), "should reject %s (%s)", name, host)
			})
		}
	})

	t.Run("allows ordinary public addresses", func(t *testing.T) {
		for name, host := range map[string]string{
			"public v4": "8.8.8.8",
			"public v6": "2001:4860:4860::8888",
		} {
			t.Run(name, func(t *testing.T) {
				require.NoError(t, util.ValidateHost(host), "should allow %s (%s)", name, host)
			})
		}
	})

	// A warehouse reached over AWS PrivateLink resolves to a private address in
	// the customer's VPC. Rejecting private space would break every such
	// connection, so it must stay allowed.
	t.Run("allows private addresses so PrivateLink keeps working", func(t *testing.T) {
		for name, host := range map[string]string{
			"private 10/8":       "10.0.0.1",
			"private 172.16/12":  "172.16.5.4",
			"private 192.168/16": "192.168.1.1",
			"unique local v6":    "fd00::1",
			// Adjacent to AWS's reserved metadata prefix but not in it — the
			// metadata guard must stay narrow enough to leave ULAs alone.
			"unique local v6, other": "fdab:1234::5",
		} {
			t.Run(name, func(t *testing.T) {
				require.NoError(t, util.ValidateHost(host), "should allow %s (%s)", name, host)
			})
		}
	})

	// AllowLoopback exists so container-backed tests can reach a local
	// database. It must not become a general bypass.
	t.Run("AllowLoopback", func(t *testing.T) {
		t.Run("permits loopback", func(t *testing.T) {
			require.NoError(t, util.ValidateHost("127.0.0.1", util.AllowLoopback(true)),
				"should allow loopback when opted in")
			require.NoError(t, util.ValidateHost("::1", util.AllowLoopback(true)),
				"should allow ipv6 loopback when opted in")
		})

		t.Run("does not relax anything else", func(t *testing.T) {
			for name, host := range map[string]string{
				"instance metadata": "169.254.169.254",
				"link-local":        "169.254.10.1",
				"multicast":         "239.1.1.1",
				"unspecified":       "0.0.0.0",
			} {
				t.Run(name, func(t *testing.T) {
					require.Error(t, util.ValidateHost(host, util.AllowLoopback(true)),
						"AllowLoopback must not permit %s (%s)", name, host)
				})
			}
		})
	})
}

type addressCase struct {
	Name         string  `json:"name"`
	Address      string  `json:"address"`
	Verdict      string  `json:"verdict"`
	Class        *string `json:"class"`
	EmbeddedIPv4 string  `json:"embeddedIPv4,omitempty"`
}

func TestSQ26_SharedFixtureRefusedSet(t *testing.T) {
	raw, err := os.ReadFile("../../clickhousequery/testdata/addresses.json")
	require.NoError(t, err)
	var f struct {
		Version int           `json:"version"`
		Cases   []addressCase `json:"cases"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(&f))
	require.Equal(t, 1, f.Version)
	require.GreaterOrEqual(t, len(f.Cases), 30)
	require.True(t, slices.IsSortedFunc(f.Cases, func(a, b addressCase) int { return strings.Compare(a.Name, b.Name) }), "cases sorted by name")
	for i := 1; i < len(f.Cases); i++ {
		require.NotEqual(t, f.Cases[i-1].Name, f.Cases[i].Name, "case names are unique")
	}
	for _, c := range f.Cases {
		ip := net.ParseIP(c.Address)
		require.NotNil(t, ip, c.Name)
		class, embedded := util.RefusedClass(ip)
		if c.Verdict == "allowed" {
			require.Nil(t, c.Class, c.Name)
			require.Empty(t, c.EmbeddedIPv4, c.Name)
			require.Empty(t, class, c.Name)
			require.Nil(t, embedded, c.Name)
			require.Empty(t, util.DisallowedAddrReason(ip), c.Name)
			continue
		}
		require.Equal(t, "refused", c.Verdict, c.Name)
		require.NotNil(t, c.Class, c.Name)
		require.Equal(t, *c.Class, class, c.Name)
		require.NotEmpty(t, util.DisallowedAddrReason(ip), c.Name)
		if c.EmbeddedIPv4 != "" {
			require.Equal(t, c.EmbeddedIPv4, embedded.String(), c.Name)
		} else {
			require.Nil(t, embedded, c.Name)
		}
	}
	again, err := json.MarshalIndent(f, "", "  ")
	require.NoError(t, err)
	require.Equal(t, string(raw), string(again)+"\n", "2-space indent, LF, trailing newline")
}

func TestEmbeddedIPv4(t *testing.T) {
	for addr, want := range map[string][]string{
		"127.0.0.1":            nil,
		"::ffff:127.0.0.1":     nil,
		"2001:4860:4860::8888": nil,
		"64:ff9b::a9fe:a9fe":   {"169.254.169.254"},
		// 64:ff9b:1::/48 yields one candidate per RFC 6052 layout: /48, /56, /64, /96.
		"64:ff9b:1:a9fe:a9:fe00::":       {"169.254.169.254", "254.169.254.0", "169.254.0.0", "0.0.0.0"},
		"64:ff9b:1::7f00:1":              {"0.0.0.0", "0.0.0.0", "0.0.0.127", "127.0.0.1"},
		"64:ff9b:1:808:8:808:808:808":    {"8.8.8.8", "8.8.8.8", "8.8.8.8", "8.8.8.8"},
		"64:ff9b:1:808:a9:fea9:fe08:808": {"8.8.169.254", "8.169.254.169", "169.254.169.254", "254.8.8.8"},
		"2002:7f00:1::":                  {"127.0.0.1"},
		"2001:0:808:808::80ff:fffe":      {"8.8.8.8", "127.0.0.1"},
	} {
		var got []string
		for _, v4 := range util.EmbeddedIPv4(net.ParseIP(addr)) {
			got = append(got, v4.String())
		}
		require.Equal(t, want, got, addr)
	}
}
