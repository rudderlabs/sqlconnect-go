package maxcompute

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

var (
	// The account contract of the source definition (HLD "Account contract").
	// The region is the host label between "service." and ".maxcompute".
	endpointPattern    = regexp.MustCompile(`^https://service\.([a-z]{2}(?:-[a-z0-9]+)+)\.maxcompute\.aliyun\.com/api$`)
	projectPattern     = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{2,27}$`)
	accessKeyIDPattern = regexp.MustCompile(`^[A-Za-z0-9]{1,128}$`)
	// A schema name is a plain identifier: MaxCompute refuses a backtick or a dot in a
	// name, and folds every name to lower case.
	schemaPattern = regexp.MustCompile(`^[A-Za-z0-9_]{1,128}$`)

	errRoleNotSupported = errors.New("maxcompute: RAM role accounts are not supported yet")
)

const maxSecretLength = 500

// Config is the MaxCompute account the driver connects with. Only AccessKey
// accounts are supported in v1 (HLD OQ4); the role keys are part of the
// contract so that a role mode can be added later without changing it.
type Config struct {
	// Endpoint is the regional public API endpoint,
	// https://service.<region>.maxcompute.aliyun.com/api.
	Endpoint string `json:"endpoint"`
	// Project is the MaxCompute project, the catalog of every relation.
	Project string `json:"project"`
	// AccessKeyID and AccessKeySecret are a RAM user's AccessKey.
	AccessKeyID     string `json:"accessKeyId"`
	AccessKeySecret string `json:"accessKeySecret"`
	// Schema is the default schema for unqualified names, sent as
	// odps.default.schema on every job. Empty means the project's default.
	Schema string `json:"schema,omitempty"`

	// RoleARN and ExternalID are reserved for the RAM role mode and refused in v1.
	RoleARN    string `json:"roleArn,omitempty"`
	ExternalID string `json:"externalId,omitempty"`

	region string
}

// Parse parses and validates the given JSON. Unknown keys fail the parse. No
// error names a value, because a mistyped key can carry a pasted secret.
func (c *Config) Parse(configJSON json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(configJSON))
	dec.DisallowUnknownFields()
	var parsed Config
	if err := dec.Decode(&parsed); err != nil {
		return errors.New("maxcompute: invalid config: it must be a JSON object with the keys endpoint, project, accessKeyId, accessKeySecret and optionally schema")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("maxcompute: invalid config: unexpected data after the JSON object")
	}
	if err := parsed.validate(); err != nil {
		return err
	}
	*c = parsed
	return nil
}

func (c *Config) validate() error {
	if c.RoleARN != "" || c.ExternalID != "" {
		return errRoleNotSupported
	}
	m := endpointPattern.FindStringSubmatch(c.Endpoint)
	if m == nil {
		return errors.New("maxcompute: invalid config: endpoint must be https://service.<region>.maxcompute.aliyun.com/api")
	}
	c.region = m[1]
	if !projectPattern.MatchString(c.Project) {
		return errors.New("maxcompute: invalid config: project must start with a letter and hold 3 to 28 letters, digits or underscores")
	}
	if !accessKeyIDPattern.MatchString(c.AccessKeyID) {
		return errors.New("maxcompute: invalid config: accessKeyId must hold 1 to 128 letters or digits")
	}
	if strings.TrimSpace(c.AccessKeySecret) == "" || len(c.AccessKeySecret) > maxSecretLength {
		return errors.New("maxcompute: invalid config: accessKeySecret must not be blank")
	}
	if c.Schema != "" {
		if !schemaPattern.MatchString(c.Schema) {
			return errors.New("maxcompute: invalid config: schema must hold only letters, digits or underscores")
		}
		c.Schema = strings.ToLower(c.Schema)
	}
	return nil
}

// Region is the region taken from the endpoint host.
func (c Config) Region() string {
	return c.region
}

// String prints the endpoint, the project and the first characters of the
// AccessKey id. It never prints the secret.
func (c Config) String() string {
	id := c.AccessKeyID
	if len(id) > 4 {
		id = id[:4]
	}
	return fmt.Sprintf("maxcompute{endpoint=%s project=%s schema=%s accessKeyId=%s…}", c.Endpoint, c.Project, c.Schema, id)
}

// GoString keeps %#v from printing the secret.
func (c Config) GoString() string {
	return c.String()
}
