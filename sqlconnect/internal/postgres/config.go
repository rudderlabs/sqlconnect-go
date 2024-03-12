package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

// Config used to connect to SQL Database
type Config struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	DBName   string `json:"dbname"`
	User     string `json:"user"`
	Password string `json:"password"`
	SSLMode  string `json:"sslmode"`

	// SkipHostValidation is used to skip host validation during tests
	SkipHostValidation bool `json:"skipHostValidation"`
	UseLegacyMappings  bool `json:"useLegacyMappings"`
}

func (c Config) ConnectionString() string {
	if c.Port == 0 {
		c.Port = 5432
	}
	sslMode := "disable"
	if c.SSLMode != "" {
		sslMode = c.SSLMode
	}
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s", c.Host, c.Port, c.DBName, c.User, c.Password, sslMode)
}

func (c *Config) Parse(input json.RawMessage) error {
	err := json.Unmarshal(input, c)
	if err != nil {
		return err
	}
	if !c.SkipHostValidation {
		return util.ValidateHost(c.Host)
	}
	return nil
}
