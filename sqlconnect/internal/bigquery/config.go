package bigquery

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/bigquery/driver"
)

type Config struct {
	ProjectID       string `json:"project"`
	CredentialsJSON string `json:"credentials"`

	UseLegacyMappings bool `json:"useLegacyMappings"`

	// Driver-level retry configuration (google-cloud-go internal retries)
	// MaxRetries limits the number of internal retry attempts for retryable errors
	// (backendError, rateLimitExceeded). Default: nil (uses google-cloud-go default, which is unlimited).
	// This is passed to google-cloud-go via bigquery.WithMaxRetries().
	MaxRetries *int `json:"maxRetries,omitempty"`

	// Application-level retry configuration (sqlconnect-go retries)
	// Optional configuration for retrying rate-limit errors that google-cloud-go
	// does NOT automatically retry (e.g., invalidQuery errors).
	RetryConfig *RetryConfig `json:"retryConfig,omitempty"`
}

// RetryConfig is the JSON configuration struct for application-level retries.
// All fields are optional pointers; unset fields use defaults from driver.DefaultRetryConfig().
type RetryConfig struct {
	// InitialInterval is the initial delay before the first retry.
	// Default: 500ms
	InitialInterval *Duration `json:"initialInterval,omitempty"`
	// RandomizationFactor adds jitter to prevent thundering herd.
	// Default: 0.5
	RandomizationFactor *float64 `json:"randomizationFactor,omitempty"`
	// Multiplier is the factor by which backoff increases after each retry.
	// Default: 1.5
	Multiplier *float64 `json:"multiplier,omitempty"`
	// MaxInterval is the maximum delay between retries.
	// Default: 60 seconds
	MaxInterval *Duration `json:"maxInterval,omitempty"`
	// MaxRetries is the maximum number of retry attempts.
	// Default: 0 (unlimited)
	MaxRetries *uint `json:"maxRetries,omitempty"`
	// MaxElapsedTime limits the total time spent retrying.
	// Default: 15 minutes
	MaxElapsedTime *Duration `json:"maxElapsedTime,omitempty"`
}

// Duration is a wrapper around time.Duration for JSON marshaling/unmarshaling
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	duration, err := time.ParseDuration(v)
	if err != nil {
		return err
	}
	*d = Duration(duration)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// ToDriverRetryConfig converts the JSON config to the driver's execution config.
// Returns nil if rc is nil (no retry configured).
func (rc *RetryConfig) ToDriverRetryConfig() *driver.RetryConfig {
	if rc == nil {
		return nil
	}
	drc := driver.DefaultRetryConfig()
	if rc.InitialInterval != nil {
		drc.InitialInterval = time.Duration(*rc.InitialInterval)
	}
	if rc.RandomizationFactor != nil {
		drc.RandomizationFactor = *rc.RandomizationFactor
	}
	if rc.Multiplier != nil {
		drc.Multiplier = *rc.Multiplier
	}
	if rc.MaxInterval != nil {
		drc.MaxInterval = time.Duration(*rc.MaxInterval)
	}
	if rc.MaxRetries != nil {
		drc.MaxRetries = *rc.MaxRetries
	}
	if rc.MaxElapsedTime != nil {
		drc.MaxElapsedTime = time.Duration(*rc.MaxElapsedTime)
	}
	return &drc
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
	return nil
}
