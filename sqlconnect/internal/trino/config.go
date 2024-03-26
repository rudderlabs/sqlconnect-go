package trino

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/trinodb/trino-go-client/trino"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

type Config struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Catalog  string `json:"catalog"`
	User     string `json:"user"`
	Password string `json:"password"`

	TunnelInfo       *sshtunnel.Config `json:"tunnel_info,omitempty"`
	customClientName string            `json:"-"`
}

func (c Config) ConnectionString() (string, error) {
	uri := func() string {
		hostport := c.Host
		if c.Port != 0 {
			hostport = fmt.Sprintf("%s:%v", c.Host, c.Port)
		}
		uri := url.URL{
			Scheme: "https",
			User:   url.UserPassword(c.User, c.Password),
			Host:   hostport,
		}
		return uri.String()
	}()
	config := trino.Config{
		ServerURI: uri,
		Catalog:   c.Catalog,
	}
	if c.TunnelInfo != nil {
		config.CustomClientName = c.customClientName
	}
	dsn, err := config.FormatDSN()
	if err != nil {
		return "", fmt.Errorf("formatting dsn: %w", err)
	}
	return dsn, nil
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
	return util.ValidateHost(c.Host)
}
