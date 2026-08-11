package mysql_test

import (
	"fmt"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
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

	t.Run("connection string", func(t *testing.T) {
		baseConfig := func(dbname string) mysql.Config {
			return mysql.Config{
				Host: "db.example.com", Port: 3306,
				User: "rudder_svc", Password: "s3cr3t",
				DBName: dbname, SSLMode: "false",
			}
		}

		// dbname is caller-supplied and part of the DSN the driver parses, so
		// values containing DSN syntax must not be able to change the settings
		// the driver ends up with. AllowAllFiles in particular must stay off.
		t.Run("dbname cannot alter driver parameters", func(t *testing.T) {
			for _, dbname := range []string{
				`prod?allowAllFiles=true&`, // the reported payload
				`prod?allowAllFiles=1`,
				`prod?allowAllFiles=true&tls=skip-verify`,
				`prod&allowAllFiles=true`,
				`prod?allowAllFiles=true#frag`,
				`prod%3FallowAllFiles=true`,
				`prod?loc=UTC&parseTime=true`,
			} {
				t.Run(dbname, func(t *testing.T) {
					dsn, err := baseConfig(dbname).ConnectionString()
					require.NoError(t, err, "it should build a connection string")

					parsed, err := mysqldriver.ParseDSN(dsn)
					require.NoError(t, err, "the driver should parse the generated dsn")

					require.False(t, parsed.AllowAllFiles,
						"allowAllFiles must never be enabled via dbname, dsn: %s", dsn)
					require.Equal(t, dbname, parsed.DBName,
						"dbname must round-trip literally, dsn: %s", dsn)
					require.NotContains(t, parsed.Params, "allowAllFiles",
						"allowAllFiles must not leak into params, dsn: %s", dsn)
				})
			}
		})

		// The DSN used to be built with this format string. Parsing both and
		// comparing proves the move to FormatDSN did not change how the driver
		// sees a benign connection — mysqldriver.NewConfig() sets defaults that
		// the old DSN left implicit, and this is what catches any drift.
		t.Run("matches the previously hand-built dsn for a benign config", func(t *testing.T) {
			c := baseConfig("analytics")

			legacy := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%s",
				c.User, c.Password, c.Host, c.Port, c.DBName, "false")
			legacyParsed, err := mysqldriver.ParseDSN(legacy)
			require.NoError(t, err, "the legacy dsn should parse")

			dsn, err := c.ConnectionString()
			require.NoError(t, err, "it should build a connection string")
			parsed, err := mysqldriver.ParseDSN(dsn)
			require.NoError(t, err, "the generated dsn should parse")

			require.Equal(t, legacyParsed, parsed,
				"the driver must see an identical config, legacy: %s, new: %s", legacy, dsn)
		})

		t.Run("tls mode reaches the driver", func(t *testing.T) {
			for sslMode, want := range map[string]string{
				"":            "false",
				"false":       "false",
				"skip-verify": "skip-verify",
			} {
				c := baseConfig("analytics")
				c.SSLMode = sslMode
				dsn, err := c.ConnectionString()
				require.NoError(t, err, "it should build a connection string for %q", sslMode)
				parsed, err := mysqldriver.ParseDSN(dsn)
				require.NoError(t, err, "the generated dsn should parse for %q", sslMode)
				require.Equal(t, want, parsed.TLSConfig, "sslmode %q", sslMode)
			}
		})

		t.Run("ipv6 host", func(t *testing.T) {
			c := baseConfig("analytics")
			c.Host = "2001:db8::1"
			dsn, err := c.ConnectionString()
			require.NoError(t, err, "it should build a connection string")
			parsed, err := mysqldriver.ParseDSN(dsn)
			require.NoError(t, err, "the driver should parse an ipv6 address, dsn: %s", dsn)
			require.Equal(t, "[2001:db8::1]:3306", parsed.Addr, "it should bracket the address")
		})

		t.Run("credentials with dsn metacharacters", func(t *testing.T) {
			c := baseConfig("analytics")
			c.User = "user@host"
			c.Password = "p@ss:w/rd?x"
			dsn, err := c.ConnectionString()
			require.NoError(t, err, "it should build a connection string")
			parsed, err := mysqldriver.ParseDSN(dsn)
			require.NoError(t, err, "the driver should parse the generated dsn, dsn: %s", dsn)
			require.Equal(t, "user@host", parsed.User, "user should round-trip")
			require.Equal(t, "p@ss:w/rd?x", parsed.Passwd, "password should round-trip")
		})
	})
}

func TestTunnelHostValidation(t *testing.T) {
	// The ssh host is caller-supplied and is an outbound connection in its own
	// right, so a benign warehouse host says nothing about where the tunnel goes.
	tunnelConfig := func(sshHost string) []byte {
		return []byte(`{"host":"8.8.8.8","dbname":"db","useSSH":true,` +
			`"sshUser":"u","sshHost":"` + sshHost + `","sshPort":"22","sshPrivateKey":"k"}`)
	}

	t.Run("rejects a tunnel host a connection should never reach", func(t *testing.T) {
		for name, sshHost := range map[string]string{
			"loopback":          "127.0.0.1",
			"instance metadata": "169.254.169.254",
			"private":           "10.0.0.1",
		} {
			t.Run(name, func(t *testing.T) {
				var config mysql.Config
				err := config.Parse(tunnelConfig(sshHost))
				require.Error(t, err, "it should reject ssh host %s (%s)", name, sshHost)
				require.ErrorContains(t, err, "ssh tunnel host",
					"the error should identify the tunnel host as the cause")
			})
		}
	})

	t.Run("accepts a public tunnel host", func(t *testing.T) {
		var config mysql.Config
		require.NoError(t, config.Parse(tunnelConfig("8.8.4.4")),
			"it should accept a public ssh host")
	})

	// Container-backed tests tunnel through a local ssh server.
	t.Run("skipHostValidation permits a loopback tunnel host", func(t *testing.T) {
		body := []byte(`{"host":"127.0.0.1","dbname":"db","skipHostValidation":true,"useSSH":true,` +
			`"sshUser":"u","sshHost":"127.0.0.1","sshPort":"22","sshPrivateKey":"k"}`)
		var config mysql.Config
		require.NoError(t, config.Parse(body), "it should permit a loopback tunnel host when opted in")
	})
}
