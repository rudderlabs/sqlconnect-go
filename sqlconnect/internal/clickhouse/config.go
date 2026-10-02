package clickhouse

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
)

// DefaultPort is the HTTPS port used when the account omits port.
const DefaultPort = 8443

// Config is the ClickHouse account configuration.
type Config struct {
	Host       string `json:"host"`
	Port       int    `json:"port,omitempty"`
	Database   string `json:"database"`
	User       string `json:"user"`
	Password   string `json:"password"`
	Secure     *bool  `json:"secure,omitempty"`
	SkipVerify bool   `json:"skipVerify,omitempty"`
}

// The texts are shared with the account schema, config-backend and Lookout.
const (
	nameErr     = "Use letters, digits and underscores, start with a letter or underscore, at most 128 characters."
	hostErr     = "Enter a hostname or a dotted-decimal IPv4 address without a scheme, port or path."
	passwordErr = "The password cannot contain control characters or start or end with whitespace."
	portErr     = "Enter a port from 1 to 65535."
	documentErr = "the account configuration is not valid"
)

const redactedPassword = "[REDACTED]"

// hostMaxLen is the account schema maxLength of host.
const hostMaxLen = 253

var (
	namePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	// The last label must start with a letter, so non-canonical IPv4 forms
	// such as 1.2.3 or 0x7f000001 match neither branch.
	hostPattern = regexp.MustCompile(`^(((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])|([A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*[A-Za-z]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)$`)
	// The rudder-integrations-config account password pattern, character for character. It lists
	// every character, because RE2 and JavaScript disagree on \s and \p{...}.
	passwordPattern = regexp.MustCompile("^[^\\x00-\\x20\\x7F-\\xA0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]([^\\x00-\\x1F\\x7F-\\x9F]*[^\\x00-\\x20\\x7F-\\xA0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff])?$")
)

var accountKeys = []string{"host", "port", "database", "user", "password", "secure", "skipVerify"}

// excludedKeys are options that other drivers or older drafts accept. An error
// names one of them; any other unknown key stays unnamed, because a key can
// carry a pasted secret.
var excludedKeys = []string{
	"protocol", "nativePort", "caCertificate", "tunnel_info", "sshHost", "cluster", "settings",
	"timeout", "allowLoopback", "allowPlainHTTP", "skipHostValidation",
}

func invalid(field, detail string) error { return cherr.New(cherr.CodeConfigInvalid, field, detail) }

// ParseConfig validates an account config. Only the installed dial policy can
// admit secure=false; without a policy, plain HTTP is refused.
func ParseConfig(raw json.RawMessage) (Config, error) {
	p, _ := chpolicy.Current()
	return parseConfig(raw, p.AllowPlainHTTP)
}

func parseConfig(raw json.RawMessage, allowPlainHTTP bool) (Config, error) {
	present, err := checkKeys(raw)
	if err != nil {
		return Config{}, err
	}
	var c Config
	if err := json.Unmarshal(raw, &c); err != nil {
		var field string
		if te := new(json.UnmarshalTypeError); errors.As(err, &te) && slices.Contains(accountKeys, te.Field) {
			field = te.Field
		}
		return Config{}, invalid(field, documentErr)
	}
	switch {
	case len(c.Host) > hostMaxLen || !hostPattern.MatchString(c.Host):
		return Config{}, invalid("host", hostErr)
	case present["port"] && (c.Port < 1 || c.Port > 65535):
		return Config{}, invalid("port", portErr)
	case !namePattern.MatchString(c.Database):
		return Config{}, invalid("database", nameErr)
	case !namePattern.MatchString(c.User):
		return Config{}, invalid("user", nameErr)
	case !passwordPattern.MatchString(c.Password):
		return Config{}, invalid("password", passwordErr)
	case c.Secure == nil:
		return Config{}, invalid("secure", "secure is required and must be true")
	case !*c.Secure && !allowPlainHTTP:
		return Config{}, invalid("secure", "secure must be true; RudderStack does not connect over plain HTTP")
	case c.SkipVerify:
		return Config{}, invalid("skipVerify", "skipVerify must be false")
	}
	return c, nil
}

// checkKeys walks the top-level object. encoding/json matches keys without
// case and keeps the last duplicate, so a key such as "Secure" or a second
// "host" could reach a field that the account schema never checked. Only the
// exact account keys, each at most once, with non-null values, pass.
func checkKeys(raw json.RawMessage) (map[string]bool, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, invalid("", documentErr)
	}
	present := make(map[string]bool, len(accountKeys))
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, invalid("", documentErr)
		}
		key, _ := tok.(string)
		if !slices.Contains(accountKeys, key) {
			if !slices.Contains(excludedKeys, key) {
				key = ""
			}
			return nil, invalid(key, "unknown field")
		}
		if present[key] {
			return nil, invalid(key, "duplicate field")
		}
		present[key] = true
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, invalid(key, documentErr)
		}
		if string(v) == "null" {
			return nil, invalid(key, "the value cannot be null")
		}
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, invalid("", documentErr)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, invalid("", documentErr)
	}
	return present, nil
}

// String redacts the password, so %v and %+v never print it.
func (c Config) String() string {
	c.Password = redactedPassword
	type plain Config
	return fmt.Sprintf("%+v", plain(c))
}

// GoString redacts the password, so %#v never prints it.
func (c Config) GoString() string {
	c.Password = redactedPassword
	type plain Config
	return fmt.Sprintf("%#v", plain(c))
}

// PortOrDefault maps an omitted port to DefaultPort, like the account schema default.
func (c Config) PortOrDefault() int {
	if c.Port == 0 {
		return DefaultPort
	}
	return c.Port
}
