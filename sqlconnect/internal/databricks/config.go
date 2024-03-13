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

	// RetryAttempts is the maximum number of retry attempte, defaults to 4
	RetryAttempts int `json:"retryAttempts"`
	// MinRetryWaitTime is the minimum time to wait before retrying, defaults to 1 second
	MinRetryWaitTime time.Duration `json:"minRetryWaitTime"`
	// MaxRetryWaitTime is the maximum time to wait before retrying, defaults to 30 seconds
	MaxRetryWaitTime time.Duration `json:"maxRetryWaitTime"`

	MaxConnIdleTime time.Duration `json:"maxConnIdleTime"`

	UseLegacyMappings bool `json:"useLegacyMappings"`
}

func (c *Config) Parse(configJson json.RawMessage) error {
	err := json.Unmarshal(configJson, c)
	if err != nil {
		return err
	}
	if c.RetryAttempts == 0 {
		c.RetryAttempts = 4
	}
	if c.MinRetryWaitTime == 0 {
		c.MinRetryWaitTime = time.Second
	}
	if c.MaxRetryWaitTime == 0 {
		c.MaxRetryWaitTime = 30 * time.Second
	}
	return nil
}
