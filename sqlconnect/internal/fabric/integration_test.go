package fabric_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/fabric"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
)

func TestFabricDB(t *testing.T) {
	configJSON, ok := os.LookupEnv("FABRIC_TEST_ENVIRONMENT_CREDENTIALS")
	if !ok {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatal("FABRIC_TEST_ENVIRONMENT_CREDENTIALS environment variable not set")
		}
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
			ExtraTests: testDropNonEmptySchema,
		},
	)
}

func testDropNonEmptySchema(t *testing.T, db sqlconnect.DB) {
	ctx := context.Background()
	schema := sqlconnect.SchemaRef{
		Name: integrationtest.GenerateTestSchema(func(identifier string) string { return identifier }),
	}
	require.NoError(t, db.CreateSchema(ctx, schema))
	t.Cleanup(func() { _ = db.DropSchema(ctx, schema) })

	table := sqlconnect.NewRelationRef("drop_schema_test", sqlconnect.WithSchema(schema.Name))
	require.NoError(t, db.CreateTestTable(ctx, table))
	require.NoError(t, db.DropSchema(ctx, schema))

	exists, err := db.SchemaExists(ctx, schema)
	require.NoError(t, err)
	require.False(t, exists)
}
