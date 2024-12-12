package databricks

import (
	"encoding/json"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
)

type Config struct {
	Host    string `json:"host"`
	Port    int    `json:"port"`
	Path    string `json:"path"`
	Token   string `json:"token"`
	Catalog string `json:"catalog"`
	Schema  string `json:"schema"`

	TunnelInfo *sshtunnel.Config `json:"tunnel_info,omitempty"`

	RetryAttempts    int           `json:"retryAttempts"`    // default: 4
	MinRetryWaitTime time.Duration `json:"minRetryWaitTime"` // default: 1s
	MaxRetryWaitTime time.Duration `json:"maxRetryWaitTime"` // default: 30s
	MaxConnIdleTime  time.Duration `json:"maxConnIdleTime"`  // default: disabled

	Timeout time.Duration `json:"timeout"` // default: no timeout

	SessionParams map[string]string `json:"sessionParams"`

	UseLegacyMappings bool `json:"useLegacyMappings"`
	// SkipColumnNormalization skips normalizing column names during ListColumns and ListColumnsForSqlQuery.
	// Databricks is returning column names case sensitive from information schema, even though it is case insensitive.
	// So, by default table names are returned normalized by databricks, whereas column names are not.
	// To avoid this inconsistency, we are normalizing column names by default.
	// If you want to skip this normalization, set this flag to true.
	SkipColumnNormalization bool `json:"skipColumnNormalisation"`
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
	if c.RetryAttempts == 0 {
		c.RetryAttempts = 4
	}
	if c.MinRetryWaitTime == 0 {
		c.MinRetryWaitTime = 1 * time.Second
	}
	if c.MaxRetryWaitTime == 0 {
		c.MaxRetryWaitTime = 30 * time.Second
	}
	return nil
}
