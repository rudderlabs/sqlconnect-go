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
