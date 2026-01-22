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

	// Driver-level retry configuration (google-cloud-go internal retries)
	// MaxRetries limits the number of internal retry attempts for retryable errors
	// (backendError, rateLimitExceeded). Default: 0 (unlimited/infinite retries).
	// This is passed to google-cloud-go via bigquery.WithMaxRetries().
	MaxRetries *int `json:"maxRetries,omitempty"`

	// Application-level retry configuration (sqlconnect-go retries)
	// QueryRetryAttempts limits retry attempts for rate-limit errors that
	// google-cloud-go does NOT automatically retry (e.g., invalidQuery errors).
	// Default: 0 (unlimited retries, relies on QueryRetryDuration or context).
	QueryRetryAttempts *int `json:"queryRetryAttempts,omitempty"`
	// QueryRetryDuration limits total time spent on application-level retries (e.g., "10m", "30s")
	// If both QueryRetryAttempts and QueryRetryDuration are set, whichever limit is hit first applies.
	QueryRetryDuration *string `json:"queryRetryDuration,omitempty"`
}

// Parse parses the given JSON into the config
func (c *Config) Parse(configJSON json.RawMessage) error {
	return json.Unmarshal(configJSON, c)
}

// Validate validates the config fields
func (c *Config) Validate() error {
	if c.MaxRetries != nil && *c.MaxRetries < 0 {
		return fmt.Errorf("maxRetries must be non-negative, got %d", *c.MaxRetries)
	}
	if c.QueryRetryAttempts != nil && *c.QueryRetryAttempts < 0 {
		return fmt.Errorf("queryRetryAttempts must be non-negative, got %d", *c.QueryRetryAttempts)
	}
	if c.QueryRetryDuration != nil {
		if _, err := time.ParseDuration(*c.QueryRetryDuration); err != nil {
			return fmt.Errorf("queryRetryDuration is not a valid duration (e.g., \"10m\", \"30s\"): %w", err)
		}
	}
	return nil
}

// GetQueryRetryDuration parses the QueryRetryDuration string into a time.Duration
// Returns nil if QueryRetryDuration is not set
// Note: Call Validate() first to ensure the duration string is valid
func (c *Config) GetQueryRetryDuration() *time.Duration {
	if c.QueryRetryDuration == nil {
		return nil
	}
	duration, err := time.ParseDuration(*c.QueryRetryDuration)
	if err != nil {
		// Should not happen if Validate() was called
		return nil
	}
	return &duration
}
