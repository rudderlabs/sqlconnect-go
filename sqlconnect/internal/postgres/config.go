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
	// UseLegacyDriver is used to use the legacy pq driver
	UseLegacyDriver bool `json:"useLegacyDriver"`
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
	if !c.SkipHostValidation {
		return util.ValidateHost(c.Host)
	}
	return nil
}
