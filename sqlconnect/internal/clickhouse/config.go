package clickhouse

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

type Config struct {
	Host            string `json:"host"`
	Port            int    `json:"port,omitempty"`
	Database        string `json:"database"`
	User            string `json:"user"`
	Password        string `json:"password"`
	Secure          *bool  `json:"secure,omitempty"`
	SkipVerify      bool   `json:"skipVerify,omitempty"`
	ScratchDatabase string `json:"scratchDatabase"`
}

func (c *Config) Parse(input json.RawMessage) error {
	var parsed Config
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parsed); err != nil {
		// Decoder errors can contain account values, including credentials.
		return adapterError("CH_CONFIG_INVALID", "invalid account fields", nil)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return adapterError("CH_CONFIG_INVALID", "expected one account object", nil)
	}
	if parsed.Host == "" || strings.ContainsAny(parsed.Host, "/\\?#@ \t\r\n") || (strings.Contains(parsed.Host, ":") && net.ParseIP(parsed.Host) == nil) {
		return adapterError("CH_CONFIG_INVALID", "invalid host", nil)
	}
	if parsed.Port == 0 {
		parsed.Port = 9440
	}
	if parsed.Port < 1 || parsed.Port > 65535 {
		return adapterError("CH_CONFIG_INVALID", "invalid port", nil)
	}
	for _, field := range []struct{ name, value string }{{"database", parsed.Database}, {"user", parsed.User}, {"scratchDatabase", parsed.ScratchDatabase}} {
		if strings.TrimSpace(field.value) == "" || strings.ContainsRune(field.value, 0) {
			return adapterError("CH_CONFIG_INVALID", "invalid "+field.name, nil)
		}
	}
	if err := util.ValidateHost(parsed.Host); err != nil {
		return adapterError("CH_CONFIG_INVALID", "invalid or unresolvable host", nil)
	}
	if parsed.Secure == nil {
		secure := true
		parsed.Secure = &secure
	}
	*c = parsed
	return nil
}

func (c Config) options() *ch.Options {
	var tlsConfig *tls.Config
	if c.Secure == nil || *c.Secure {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.Host, InsecureSkipVerify: c.SkipVerify} //nolint:gosec // Explicit account option for the self-signed mini fixture.
	}
	return &ch.Options{
		Protocol:         ch.Native,
		Addr:             []string{net.JoinHostPort(c.Host, strconv.Itoa(c.Port))},
		Auth:             ch.Auth{Database: c.Database, Username: c.User, Password: c.Password},
		TLS:              tlsConfig,
		DialTimeout:      90 * time.Second,
		ReadTimeout:      300 * time.Second,
		MaxOpenConns:     10,
		MaxIdleConns:     5,
		ConnMaxLifetime:  30 * time.Minute,
		ConnOpenStrategy: ch.ConnOpenInOrder,
		BlockBufferSize:  2,
		Compression:      &ch.Compression{Method: ch.CompressionLZ4},
		Settings:         defaultSettings(),
	}
}
