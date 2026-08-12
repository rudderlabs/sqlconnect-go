package sshtunnel

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

// Config represents the configuration for an SSH tunnel.
type Config struct {
	User       string `json:"sshUser"`
	Host       string `json:"sshHost"`
	Port       string `json:"sshPort"`
	PrivateKey string `json:"sshPrivateKey"`
}

// Validate checks if the Config is valid.
func (c Config) Validate() error {
	if c.User == "" {
		return fmt.Errorf("ssh user is required")
	}
	if c.Host == "" {
		return fmt.Errorf("ssh host is required")
	}
	if c.Port == "" {
		return fmt.Errorf("ssh port is required")
	}
	if _, err := strconv.Atoi(c.Port); err != nil {
		return fmt.Errorf("invalid port: %s", c.Port)
	}
	if c.PrivateKey == "" {
		return fmt.Errorf("ssh private key is required")
	}
	return nil
}

// ParseInlineConfig parses the given data as a JSON object and returns a Config if the "useSSH" field is true.
func ParseInlineConfig(data []byte) (*Config, error) {
	if gjson.GetBytes(data, "useSSH").Bool() {
		var c Config
		err := json.Unmarshal(data, &c)
		return &c, err
	}
	return nil, nil // nolint: nilnil
}

// ValidateHost validates the tunnel endpoint, if a tunnel is configured.
//
// The ssh host is caller-supplied like every other connection field, and the
// tunnel is an outbound connection in its own right — so a benign warehouse
// host says nothing about where the tunnel actually goes. Connectors pass the
// same options they use for the warehouse host, so a config permitted to use
// loopback for the database may use it for the tunnel too.
func ValidateHost(c *Config, opts ...util.HostValidationOption) error {
	if c == nil {
		return nil
	}
	if err := util.ValidateHost(c.Host, opts...); err != nil {
		return fmt.Errorf("ssh tunnel host: %w", err)
	}
	return nil
}
