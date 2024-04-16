package redshift

import (
	"encoding/json"
	"time"

	"github.com/tidwall/sjson"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/postgres"
)

const RedshiftDataConfigType = "redshift-data"

// PostgresConfig is the configuration for a redshift database when using the postgres driver
type PostgresConfig = postgres.Config

// Config is the configuration for a redshift database when using the redshift data api driver
type Config struct {
	ClusterIdentifier string `json:"clusterIdentifier"`
	Database          string `json:"database"`
	User              string `json:"user"`
	Region            string `json:"region"`
	WorkgroupName     string `json:"workgroupName"`

	SecretsARN string `json:"secretsARN"`

	SharedConfigProfile string `json:"sharedConfigProfile"`

	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	SessionToken    string `json:"sessionToken"`

	Timeout          time.Duration `json:"timeout"`          // default: no timeout
	MinPolling       time.Duration `json:"minPolling"`       // default: 10ms
	MaxPolling       time.Duration `json:"maxPolling"`       // default: 5s
	RetryMaxAttempts int           `json:"retryMaxAttempts"` // default: 20

	UseLegacyMappings bool `json:"useLegacyMappings"`
}

func (c *Config) MarshalJSON() ([]byte, error) {
	bytes, err := json.Marshal(*c)
	if err != nil {
		return nil, err
	}
	return sjson.SetBytes(bytes, "type", RedshiftDataConfigType)
}

func (c *Config) Parse(input json.RawMessage) error {
	err := json.Unmarshal(input, c)
	if err != nil {
		return err
	}
	return nil
}
