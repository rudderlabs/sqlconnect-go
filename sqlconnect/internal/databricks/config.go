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

	// RudderSchema is used to override the default rudder schema name during tests
	RudderSchema      string `json:"rudderSchema"`
	UseLegacyMappings bool   `json:"useLegacyMappings"`
}

func (c *Config) Parse(configJson json.RawMessage) error {
	err := json.Unmarshal(configJson, c)
	if err != nil {
		return err
	}
	// if catalog is empty from the UI, use default "hive_metastore"
	if c.Catalog == "" {
		c.Catalog = "hive_metastore"
	}
	return nil
}
