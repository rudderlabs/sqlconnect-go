package mysql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/mysql"
)

func TestConfig(t *testing.T) {
	t.Run("host validation", func(t *testing.T) {
		var config mysql.Config
		err := config.Parse([]byte(`{"host": "localhost"}`))
		require.Error(t, err, "should not allow localhost")

		err = config.Parse([]byte(`{"host": "127.0.0.1"}`))
		require.Error(t, err, "should not allow 127.0.0.1")

		err = config.Parse([]byte(`{"host": "0.0.0.0"}`))
		require.Error(t, err, "should not allow 0.0.0.0")
	})

	t.Run("tls", func(t *testing.T) {
		t.Run("empty ssl mode", func(t *testing.T) {
			c := mysql.Config{SSLMode: ""}
			tls, err := c.TLS()
			require.NoError(t, err, "should allow empty tls")
			require.Equal(t, "false", tls, "should return false")
		})

		t.Run("skip-verify ssl mode", func(t *testing.T) {
			c := mysql.Config{SSLMode: "skip-verify"}
			tls, err := c.TLS()
			require.NoError(t, err, "should allow skip-verify tls")
			require.Equal(t, "skip-verify", tls, "should return skip-verify")
		})
		t.Run("false ssl mode", func(t *testing.T) {
			c := mysql.Config{SSLMode: "false"}
			tls, err := c.TLS()
			require.NoError(t, err, "should allow false tls")
			require.Equal(t, "false", tls, "should return false")
		})

		t.Run("other ssl mode", func(t *testing.T) {
			c := mysql.Config{SSLMode: "other"}
			_, err := c.TLS()
			require.Error(t, err, "should not allow other tls")
		})
	})
}
