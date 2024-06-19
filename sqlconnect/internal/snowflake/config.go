package snowflake

import (
	"crypto/rsa"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	"github.com/snowflakedb/gosnowflake"
	"github.com/youmark/pkcs8"
)

type Config struct {
	Account   string `json:"account"`
	Warehouse string `json:"warehouse"`
	DBName    string `json:"dbname"`
	User      string `json:"user"`
	Schema    string `json:"schema"`
	Role      string `json:"role"`

	Password string `json:"password"`

	UseKeyPairAuth       bool   `json:"useKeyPairAuth"`
	PrivateKey           string `json:"privateKey"`
	PrivateKeyPassphrase string `json:"privateKeyPassphrase"`

	KeepSessionAlive  bool `json:"keepSessionAlive"`
	UseLegacyMappings bool `json:"useLegacyMappings"`
}

func (c Config) ConnectionString() (dsn string, err error) {
	sc := gosnowflake.Config{
		Authenticator: gosnowflake.AuthTypeSnowflake,
		User:          c.User,
		Password:      c.Password,
		Account:       c.Account,
		Database:      c.DBName,
		Warehouse:     c.Warehouse,
		Schema:        c.Schema,
		Role:          c.Role,
	}

	if c.UseKeyPairAuth {
		sc.Authenticator = gosnowflake.AuthTypeJwt
		privateKey, err := c.ParsePrivateKey()
		if err != nil {
			return "", fmt.Errorf("parsing private key: %w", err)
		}
		sc.PrivateKey = privateKey
	}

	if c.KeepSessionAlive {
		params := make(map[string]*string)
		valueTrue := "true"
		params["client_session_keep_alive"] = &valueTrue
		sc.Params = params
	}

	dsn, err = gosnowflake.DSN(&sc)
	if err != nil {
		err = fmt.Errorf("creating dsn: %v", err)
	}
	return
}

func (c *Config) Parse(configJSON json.RawMessage) error {
	return json.Unmarshal(configJSON, c)
}

func (c *Config) ParsePrivateKey() (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(normalisePem(c.PrivateKey)))
	if block == nil {
		return nil, errors.New("decoding private key failed")
	}

	var opts [][]byte
	if len(c.PrivateKeyPassphrase) > 0 {
		opts = append(opts, []byte(c.PrivateKeyPassphrase))
	}

	rsaPrivateKey, err := pkcs8.ParsePKCS8PrivateKeyRSA(block.Bytes, opts...)
	if err != nil {
		return nil, fmt.Errorf("parsing private key: %w", err)
	}
	return rsaPrivateKey, nil
}

// normalisePem formats the content of certificates and keys by adding necessary newlines around specific markers.
func normalisePem(content string) string {
	// Remove all existing newline characters and replace them with a space
	formattedContent := strings.ReplaceAll(content, "\n", " ")

	// Add a newline after specific BEGIN markers
	formattedContent = strings.Replace(formattedContent, "-----BEGIN CERTIFICATE-----", "-----BEGIN CERTIFICATE-----\n", 1)
	formattedContent = strings.Replace(formattedContent, "-----BEGIN RSA PRIVATE KEY-----", "-----BEGIN RSA PRIVATE KEY-----\n", 1)
	formattedContent = strings.Replace(formattedContent, "-----BEGIN ENCRYPTED PRIVATE KEY-----", "-----BEGIN ENCRYPTED PRIVATE KEY-----\n", 1)
	formattedContent = strings.Replace(formattedContent, "-----BEGIN PRIVATE KEY-----", "-----BEGIN PRIVATE KEY-----\n", 1)

	// Add a newline before and after specific END markers
	formattedContent = strings.Replace(formattedContent, "-----END CERTIFICATE-----", "\n-----END CERTIFICATE-----\n", 1)
	formattedContent = strings.Replace(formattedContent, "-----END RSA PRIVATE KEY-----", "\n-----END RSA PRIVATE KEY-----\n", 1)
	formattedContent = strings.Replace(formattedContent, "-----END ENCRYPTED PRIVATE KEY-----", "\n-----END ENCRYPTED PRIVATE KEY-----\n", 1)
	formattedContent = strings.Replace(formattedContent, "-----END PRIVATE KEY-----", "\n-----END PRIVATE KEY-----\n", 1)

	return formattedContent
}
