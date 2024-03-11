package bigquery

import (
	"encoding/json"
)

type Config struct {
	ProjectID       string `json:"project"`
	CredentialsJSON string `json:"credentials"`

	UseLegacyMappings bool `json:"useLegacyMappings"`
}

// Parse parses the given JSON into the config
func (c *Config) Parse(configJSON json.RawMessage) error {
	return json.Unmarshal(configJSON, c)
}
