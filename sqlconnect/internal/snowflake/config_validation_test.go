package snowflake_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/snowflake"
)

func TestConfigParseValidation(t *testing.T) {
	t.Run("protocol", func(t *testing.T) {
		// The driver honours any protocol other than https by passing it
		// through as a connection parameter, which would put credentials on
		// the wire in cleartext.
		t.Run("rejects anything that is not https", func(t *testing.T) {
			for _, protocol := range []string{"http", "HTTP", "ftp", "https://"} {
				t.Run(protocol, func(t *testing.T) {
					var config snowflake.Config
					err := config.Parse([]byte(`{"account":"acct","protocol":"` + protocol + `"}`))
					require.Error(t, err, "it should reject protocol %q", protocol)
					require.ErrorContains(t, err, "unsupported protocol")
				})
			}
		})

		t.Run("accepts https and empty", func(t *testing.T) {
			for name, body := range map[string]string{
				"https":   `{"account":"acct","protocol":"https"}`,
				"empty":   `{"account":"acct","protocol":""}`,
				"omitted": `{"account":"acct"}`,
			} {
				t.Run(name, func(t *testing.T) {
					var config snowflake.Config
					require.NoError(t, config.Parse([]byte(body)), "it should accept %s", name)
				})
			}
		})
	})

	t.Run("host", func(t *testing.T) {
		t.Run("rejects addresses a connection should never reach", func(t *testing.T) {
			for name, host := range map[string]string{
				"loopback":          "127.0.0.1",
				"instance metadata": "169.254.169.254",
			} {
				t.Run(name, func(t *testing.T) {
					var config snowflake.Config
					err := config.Parse([]byte(`{"account":"acct","host":"` + host + `"}`))
					require.Error(t, err, "it should reject %s (%s)", name, host)
				})
			}
		})

		// Host is optional — the driver derives it from Account — so an absent
		// host must not be treated as an invalid one.
		t.Run("absent host is not validated", func(t *testing.T) {
			var config snowflake.Config
			require.NoError(t, config.Parse([]byte(`{"account":"acct"}`)),
				"it should accept a config without a host")
		})

		t.Run("accepts a public host", func(t *testing.T) {
			var config snowflake.Config
			require.NoError(t, config.Parse([]byte(`{"account":"acct","host":"8.8.8.8"}`)),
				"it should accept a public host")
		})
	})

	t.Run("invalid json", func(t *testing.T) {
		var config snowflake.Config
		require.Error(t, config.Parse([]byte(`{"account":`)), "it should reject malformed json")
	})
}
