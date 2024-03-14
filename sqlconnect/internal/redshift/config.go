package redshift

import (
	"encoding/json"
	"time"

	"github.com/tidwall/sjson"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/postgres"
)

const SDKConfigType = "sdk"

// Config is the configuration for a redshift database when using postgres driver
type Config = postgres.Config

// SDKConfig is the configuration for a redshift database when using the AWS SDK
type SDKConfig struct {
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

	Timeout    time.Duration `json:"timeout"`    // default: no timeout
	MinPolling time.Duration `json:"minPolling"` // default: 10ms
	MaxPolling time.Duration `json:"manPolling"` // default: 5s

	UseLegacyMappings bool `json:"useLegacyMappings"`
}

func (c *SDKConfig) MarshalJSON() ([]byte, error) {
	bytes, err := json.Marshal(*c)
	if err != nil {
		return nil, err
	}
	return sjson.SetBytes(bytes, "type", SDKConfigType)
}

func (c *SDKConfig) Parse(input json.RawMessage) error {
	err := json.Unmarshal(input, c)
	if err != nil {
		return err
	}
	return nil
}
