package bigquery

import (
	"encoding/json"
	"fmt"
)

type Config struct {
	ProjectID       string `json:"project"`
	CredentialsJSON string `json:"credentials"`

	UseLegacyMappings bool `json:"useLegacyMappings"`
}

// Parse parses and validates the given JSON as a service-account configuration.
func (c *Config) Parse(configJSON json.RawMessage) error {
	return c.parse(configJSON, true)
}

func (c *Config) parse(configJSON json.RawMessage, validateCredentials bool) error {
	if err := json.Unmarshal(configJSON, c); err != nil {
		return err
	}
	if validateCredentials {
		if err := validateServiceAccountJSON([]byte(c.CredentialsJSON)); err != nil {
			return fmt.Errorf("validating bigquery credentials: %w", err)
		}
	}
	return nil
}
