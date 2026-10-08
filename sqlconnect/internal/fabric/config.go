package fabric

import (
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/microsoft/go-mssqldb/azuread"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

const sqlEndpointPort = 1433

// Config contains the credentials required to connect to a Microsoft Fabric SQL endpoint.
type Config struct {
	Host              string `json:"host"`
	Database          string `json:"database"`
	TenantID          string `json:"tenantId"`
	ClientID          string `json:"clientId"`
	ClientSecret      string `json:"clientSecret"`
	FabricWorkspaceID string `json:"fabricWorkspaceId,omitempty"`
	// Timeout bounds SQL connection dialing and the optional Fabric REST
	// bootstrap. It does not impose a timeout on SQL query execution.
	Timeout time.Duration `json:"timeout"`
}

// Parse decodes and validates a Fabric configuration.
func (c *Config) Parse(input json.RawMessage) error {
	if err := json.Unmarshal(input, c); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"host":         c.Host,
		"database":     c.Database,
		"tenantId":     c.TenantID,
		"clientId":     c.ClientID,
		"clientSecret": c.ClientSecret,
	} {
		if strings.TrimSpace(value) == "" {
			return errors.New(name + " is required")
		}
	}
	return util.ValidateHost(c.Host)
}

// ConnectionString returns the go-mssqldb Azure AD connection string. Fabric
// SQL endpoints always use port 1433.
func (c Config) ConnectionString() string {
	query := url.Values{}
	query.Set("database", c.Database)
	query.Set("fedauth", azuread.ActiveDirectoryServicePrincipal)
	query.Set("encrypt", "true")
	if c.Timeout > 0 {
		seconds := c.Timeout / time.Second
		if c.Timeout%time.Second != 0 {
			seconds++
		}
		query.Set("dial timeout", strconv.FormatInt(int64(seconds), 10))
	}
	return (&url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(c.ClientID+"@"+c.TenantID, c.ClientSecret),
		Host:     net.JoinHostPort(c.Host, strconv.Itoa(sqlEndpointPort)),
		RawQuery: query.Encode(),
	}).String()
}
