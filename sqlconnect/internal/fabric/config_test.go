package fabric

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/microsoft/go-mssqldb/azuread"
	"github.com/stretchr/testify/require"
)

func TestConfigParse(t *testing.T) {
	configJSON := json.RawMessage(`{
		"host":"localhost",
		"database":"warehouse",
		"tenantId":"tenant",
		"clientId":"client",
		"clientSecret":"secret",
		"fabricWorkspaceId":"11111111-1111-1111-1111-111111111111",
		"timeout":1500000000,
		"skipHostValidation":true
	}`)
	var config Config
	require.NoError(t, config.Parse(configJSON))
	require.Equal(t, "localhost", config.Host)
	require.Equal(t, 1500*time.Millisecond, config.Timeout)

	for _, field := range []string{"host", "database", "tenantId", "clientId", "clientSecret"} {
		t.Run("requires "+field, func(t *testing.T) {
			var values map[string]any
			require.NoError(t, json.Unmarshal(configJSON, &values))
			values[field] = " "
			input, err := json.Marshal(values)
			require.NoError(t, err)
			require.ErrorContains(t, config.Parse(input), field+" is required")
		})
	}
}

func TestConfigParseRejectsInvalidWorkspaceID(t *testing.T) {
	var config Config
	err := config.Parse(json.RawMessage(`{
		"host":"localhost", "database":"warehouse", "tenantId":"tenant",
		"clientId":"client", "clientSecret":"secret", "fabricWorkspaceId":"../admin",
		"skipHostValidation":true
	}`))
	require.ErrorContains(t, err, "fabricWorkspaceId must be a valid UUID")
}

func TestConnectionString(t *testing.T) {
	config := Config{
		Host:         "configured.fabric.example",
		Database:     "warehouse/name",
		TenantID:     "tenant",
		ClientID:     "client+id",
		ClientSecret: "secret:@/value",
		Timeout:      1500 * time.Millisecond,
	}

	parsed, err := url.Parse(config.ConnectionString())
	require.NoError(t, err)
	require.Equal(t, "sqlserver", parsed.Scheme)
	require.Equal(t, "configured.fabric.example:1433", parsed.Host)
	require.Equal(t, "client+id@tenant", parsed.User.Username())
	password, ok := parsed.User.Password()
	require.True(t, ok)
	require.Equal(t, "secret:@/value", password)
	require.Equal(t, "warehouse/name", parsed.Query().Get("database"))
	require.Equal(t, azuread.ActiveDirectoryServicePrincipal, parsed.Query().Get("fedauth"))
	require.Equal(t, "true", parsed.Query().Get("encrypt"))
	require.Equal(t, "2", parsed.Query().Get("dial timeout"))

	config.Timeout = 500 * time.Millisecond
	parsed, err = url.Parse(config.ConnectionString())
	require.NoError(t, err)
	require.Equal(t, "1", parsed.Query().Get("dial timeout"))

	config.Timeout = 0
	parsed, err = url.Parse(config.ConnectionString())
	require.NoError(t, err)
	require.NotContains(t, parsed.Query(), "dial timeout")
}

func TestAzureADConnectorContract(t *testing.T) {
	config := Config{Host: "example.com", Database: "warehouse", TenantID: "tenant", ClientID: "client", ClientSecret: "secret"}
	connector, err := azuread.NewConnector(config.ConnectionString())
	require.NoError(t, err)
	require.NotNil(t, connector)
}
