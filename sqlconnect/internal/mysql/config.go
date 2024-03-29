package mysql

import (
	"encoding/json"
	"fmt"

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

func (c Config) ConnectionString() (string, error) {
	tls, err := c.TLS()
	if err != nil {
		return "", fmt.Errorf("error while creating connecton string, %w", err)
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%s", c.User, c.Password, c.Host, c.Port, c.DBName, tls), nil
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
	if !c.SkipHostValidation {
		return util.ValidateHost(c.Host)
	}
	return nil
}
