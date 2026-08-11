package postgres

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

// Config used to connect to SQL Database
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

func (c Config) ConnectionString() string {
	if c.Port == 0 {
		c.Port = 5432
	}
	sslMode := "disable"
	if c.SSLMode != "" {
		sslMode = c.SSLMode
	}
	dsn := url.URL{
		Scheme: DatabaseType,
		User:   url.UserPassword(c.User, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   c.DBName,
	}
	values := dsn.Query()
	values.Set("sslmode", sslMode)
	dsn.RawQuery = values.Encode()
	return dsn.String()
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
