package bigquery

import (
	"encoding/json"
	"fmt"
	"time"
)

type Config struct {
	ProjectID       string `json:"project"`
	CredentialsJSON string `json:"credentials"`

	UseLegacyMappings bool `json:"useLegacyMappings"`

	// Retry configuration for BigQuery API calls
	// MaxRetries limits the number of retry attempts (default: unlimited if not set)
	MaxRetries *int `json:"maxRetries,omitempty"`
	// MaxRetryDuration limits total time spent retrying (e.g., "10m", "30s")
	// If both MaxRetries and MaxRetryDuration are set, whichever limit is hit first applies
	MaxRetryDuration *string `json:"maxRetryDuration,omitempty"`
}

// Parse parses the given JSON into the config
func (c *Config) Parse(configJSON json.RawMessage) error {
	if err := json.Unmarshal(configJSON, c); err != nil {
		return err
	}
	if err := validateServiceAccountJSON([]byte(c.CredentialsJSON)); err != nil {
		return fmt.Errorf("validating bigquery credentials: %w", err)
	}
	return nil
}

// GetMaxRetryDuration parses the MaxRetryDuration string into a time.Duration
// Returns nil if MaxRetryDuration is not set or invalid
func (c *Config) GetMaxRetryDuration() *time.Duration {
	if c.MaxRetryDuration == nil {
		return nil
	}
	duration, err := time.ParseDuration(*c.MaxRetryDuration)
	if err != nil {
		return nil
	}
	return &duration
}
