package mysql_test

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	mysqlresource "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/mysql"

	integrationtest "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/integration_test"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/mysql"
)

func TestMysqlDB(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "it should be able to create a docker pool")

	mysqlResource, err := mysqlresource.Setup(pool, t)
	require.NoError(t, err, "it should be able to create a mysql resource")
	port, err := strconv.Atoi(mysqlResource.Port)
	require.NoError(t, err, "it should be able to convert the port to int")
	config := mysql.Config{
		Host:               mysqlResource.Host,
		Port:               port,
		DBName:             mysqlResource.Database,
		User:               mysqlResource.User,
		Password:           mysqlResource.Password,
		SSLMode:            "false",
		SkipHostValidation: true,
	}
	configJSON, err := json.Marshal(config)
	require.NoError(t, err, "it should be able to marshal config to json")

	integrationtest.TestDatabaseScenarios(
		t,
		mysql.DatabaseType,
		configJSON,
		strings.ToLower,
		integrationtest.Options{
			LegacySupport: true,
		},
	)

	integrationtest.TestSshTunnelScenarios(t, mysql.DatabaseType, configJSON)
}
