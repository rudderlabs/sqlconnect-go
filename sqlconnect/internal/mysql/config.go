package mysql

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

type Config struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	DBName   string `json:"dbname"`
	User     string `json:"user"`
	Password string `json:"password"`
	SSLMode  string `json:"sslmode"`

	TunnelInfo *sshtunnel.Config `json:"tunnel_info,omitempty"`

	// SkipHostValidation is used to skip host validation during tests
	SkipHostValidation bool `json:"skipHostValidation"`
	UseLegacyMappings  bool `json:"useLegacyMappings"`
}

// ConnectionString builds the go-sql-driver DSN from typed fields.
//
// Do not rewrite this to format the DSN by hand. Every value here comes from
// caller-supplied connection config, and the driver reads part of the DSN as
// connection parameters, so an unescaped field can change the settings the
// driver ends up with. FormatDSN escapes each field for us.
func (c Config) ConnectionString() (string, error) {
	tls, err := c.TLS()
	if err != nil {
		return "", fmt.Errorf("creating connection string: %w", err)
	}
	cfg := mysqldriver.NewConfig()
	cfg.User = c.User
	cfg.Passwd = c.Password
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	cfg.DBName = c.DBName
	cfg.TLSConfig = tls
	// Pinned explicitly: reading local files on behalf of the server is never
	// wanted here, and the connection config is caller-supplied.
	cfg.AllowAllFiles = false
	return cfg.FormatDSN(), nil
}

func (c Config) TLS() (string, error) {
	switch c.SSLMode {
	case "skip-verify", "false":
		return c.SSLMode, nil
	case "":
		return "false", nil
	default:
		return "", fmt.Errorf("sslmode %s for mysql connection is not supported", c.SSLMode)
	}
}

func (c *Config) Parse(input json.RawMessage) error {
	err := json.Unmarshal(input, c)
	if err != nil {
		return err
	}
	if c.TunnelInfo == nil { // if tunnel info is not provided as a separate json object, try to parse it from the input
		if c.TunnelInfo, err = sshtunnel.ParseInlineConfig(input); err != nil {
			return err
		}
	}
	// SkipHostValidation used to bypass validation entirely. It now only
	// permits loopback, which is all a container-backed test needs —
	// link-local, private and unspecified addresses stay rejected whether or
	// not it is set.
	if err := util.ValidateHost(c.Host, util.AllowLoopback(c.SkipHostValidation)); err != nil {
		return err
	}
	return c.validateTunnelHost()
}

// validateTunnelHost applies the same rules to the ssh tunnel endpoint.
// It is caller-supplied too and is an outbound connection in its own right,
// so a benign warehouse host says nothing about where the tunnel goes.
func (c Config) validateTunnelHost() error {
	if c.TunnelInfo == nil {
		return nil
	}
	if err := util.ValidateHost(c.TunnelInfo.Host, util.AllowLoopback(c.SkipHostValidation)); err != nil {
		return fmt.Errorf("ssh tunnel host: %w", err)
	}
	return nil
}
