package fabric_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/fabric"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
)

func TestFabricDB(t *testing.T) {
	configJSON, ok := os.LookupEnv("FABRIC_TEST_ENVIRONMENT_CREDENTIALS")
	if !ok {
		t.Skip("skipping Fabric integration test due to lack of a test environment")
	}

	var credentials map[string]any
	require.NoError(t, json.Unmarshal([]byte(configJSON), &credentials))
	// The shared SQL scenario should validate SQL endpoint behavior only. The
	// optional REST bootstrap path has focused unit coverage and should not make
	// these scenarios depend on Fabric API tenant/workspace permissions.
	delete(credentials, "fabricWorkspaceId")
	configJSONWithoutBootstrap, err := json.Marshal(credentials)
	require.NoError(t, err)

	integrationtest.TestDatabaseScenarios(
		t,
		fabric.DatabaseType,
		configJSONWithoutBootstrap,
		func(identifier string) string { return identifier },
		integrationtest.Options{
			SpecialCharactersInQuotedTable: " _A-",
			DateOf: func(column string) string {
				return "CAST(" + column + " AS DATE)"
			},
		},
	)
}
