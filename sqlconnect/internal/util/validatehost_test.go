package util_test

import (
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
			"private 10/8":       "10.0.0.1",
			"private 172.16/12":  "172.16.5.4",
			"private 192.168/16": "192.168.1.1",
			"unique local v6":    "fd00::1",
			"multicast":          "239.1.1.1",
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

	// AllowLoopback exists so container-backed tests can reach a local
	// database. It must not become a general bypass.
	t.Run("AllowLoopback", func(t *testing.T) {
		t.Run("permits loopback", func(t *testing.T) {
			require.NoError(t, util.ValidateHost("127.0.0.1", util.AllowLoopback()),
				"should allow loopback when opted in")
			require.NoError(t, util.ValidateHost("::1", util.AllowLoopback()),
				"should allow ipv6 loopback when opted in")
		})

		t.Run("does not relax anything else", func(t *testing.T) {
			for name, host := range map[string]string{
				"instance metadata": "169.254.169.254",
				"link-local":        "169.254.10.1",
				"private 10/8":      "10.0.0.1",
				"private 192.168":   "192.168.1.1",
				"unique local v6":   "fd00::1",
				"unspecified":       "0.0.0.0",
			} {
				t.Run(name, func(t *testing.T) {
					require.Error(t, util.ValidateHost(host, util.AllowLoopback()),
						"AllowLoopback must not permit %s (%s)", name, host)
				})
			}
		})
	})
}
