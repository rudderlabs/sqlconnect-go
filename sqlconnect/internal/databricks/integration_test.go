package databricks_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/databricks"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
)

func TestDatabricksDB(t *testing.T) {

	t.Run("with oauth", func(t *testing.T) {
		oauthConfigJSON, ok := os.LookupEnv("DATABRICKS_OAUTH_TEST_ENVIRONMENT_CREDENTIALS")
		if !ok {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatal("DATABRICKS_OAUTH_TEST_ENVIRONMENT_CREDENTIALS environment variable not set")
			}
			t.Skip("skipping databricks ouath integration test due to lack of a test environment")
		}
		// require.NotEmpty(t, oauthConfigJSON, "DATABRICKS_OAUTH_TEST_ENVIRONMENT_CREDENTIALS environment variable is empty")
		var oauthConfig databricks.Config
		err := json.Unmarshal([]byte(oauthConfigJSON), &oauthConfig)
		require.NoError(t, err, "failed to unmarshal oauth config")
		require.NotEmpty(t, oauthConfig.Host, "Host is empty")
		configJSON, err := sjson.Set(oauthConfigJSON, "useOauth", true)
		require.NoError(t, err, "failed to set useOauth")
		_, err = sqlconnect.NewDB(databricks.DatabaseType, []byte(configJSON))
		require.NoError(t, err, "failed to create db")

		integrationtest.TestDatabaseScenarios(
			t,
			databricks.DatabaseType,
			[]byte(configJSON),
			strings.ToLower,
			integrationtest.Options{
				LegacySupport:                  true,
				SpecialCharactersInQuotedTable: "_A",
			},
		)
	})
}
