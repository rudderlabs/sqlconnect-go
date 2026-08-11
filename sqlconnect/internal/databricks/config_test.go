package databricks_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/databricks"
)

func TestConfigHostValidation(t *testing.T) {
	t.Run("rejects addresses a connection should never reach", func(t *testing.T) {
		for name, host := range map[string]string{
			"loopback":          "127.0.0.1",
			"instance metadata": "169.254.169.254",
			"private":           "10.0.0.1",
		} {
			t.Run(name, func(t *testing.T) {
				var config databricks.Config
				require.Error(t, config.Parse([]byte(`{"host":"`+host+`"}`)),
					"it should reject %s (%s)", name, host)
			})
		}
	})

	t.Run("accepts a public host", func(t *testing.T) {
		var config databricks.Config
		require.NoError(t, config.Parse([]byte(`{"host":"8.8.8.8"}`)),
			"it should accept a public host")
	})

	// skipHostValidation exists so tests can reach a database in a local
	// container. It must not become a general bypass.
	t.Run("skipHostValidation", func(t *testing.T) {
		t.Run("permits loopback", func(t *testing.T) {
			var config databricks.Config
			require.NoError(t, config.Parse([]byte(`{"host":"127.0.0.1","skipHostValidation":true}`)),
				"it should permit loopback when opted in")
		})

		t.Run("does not relax anything else", func(t *testing.T) {
			for name, host := range map[string]string{
				"instance metadata": "169.254.169.254",
				"link-local":        "169.254.10.1",
				"private":           "10.0.0.1",
				"unspecified":       "0.0.0.0",
			} {
				t.Run(name, func(t *testing.T) {
					var config databricks.Config
					require.Error(t, config.Parse([]byte(`{"host":"`+host+`","skipHostValidation":true}`)),
						"skipHostValidation must not permit %s (%s)", name, host)
				})
			}
		})
	})
}
