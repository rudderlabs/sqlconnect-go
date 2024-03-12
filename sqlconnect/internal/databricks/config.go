package databricks

import (
	"encoding/json"
	"time"
)

type Config struct {
	Host    string `json:"host"`
	Port    int    `json:"port"`
	Path    string `json:"path"`
	Token   string `json:"token"`
	Catalog string `json:"catalog"`

	RetryAttempts    int           `json:"retryAttempts"`
	MinRetryWaitTime time.Duration `json:"minRetryWaitTime"`
	MaxRetryWaitTime time.Duration `json:"maxRetryWaitTime"`
	MaxConnIdleTime  time.Duration `json:"maxConnIdleTime"`

	UseLegacyMappings bool `json:"useLegacyMappings"`
}

func (c *Config) Parse(configJson json.RawMessage) error {
	err := json.Unmarshal(configJson, c)
	if err != nil {
		return err
	}
	if c.Catalog == "" {
		c.Catalog = "hive_metastore" // default catalog
	}
	return nil
}
