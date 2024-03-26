package postgres_test

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	pgresource "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/postgres"
)

func TestPostgresDB(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "it should be able to create a docker pool")

	postgresResource, err := pgresource.Setup(pool, t)
	require.NoError(t, err, "it should be able to create a postgres resource")
	port, err := strconv.Atoi(postgresResource.Port)
	require.NoError(t, err, "it should be able to convert postgres port to int")
	config := postgres.Config{
		Host:               postgresResource.Host,
		Port:               port,
		DBName:             postgresResource.Database,
		User:               postgresResource.User,
		Password:           postgresResource.Password,
		SSLMode:            "disable",
		SkipHostValidation: true,
	}
	configJSON, err := json.Marshal(config)
	require.NoError(t, err, "it should be able to marshal config to json")

	integrationtest.TestDatabaseScenarios(t, postgres.DatabaseType, configJSON, strings.ToLower, integrationtest.Options{LegacySupport: true})

	integrationtest.TestSshTunnelScenarios(t, postgres.DatabaseType, configJSON)
}
