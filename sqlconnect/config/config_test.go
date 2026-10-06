package config_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	sqlconnectconfig "github.com/rudderlabs/sqlconnect-go/sqlconnect/config"
)

func TestFabricAliasRegistersFactory(t *testing.T) {
	configJSON, err := json.Marshal(sqlconnectconfig.Fabric{
		Host:         "8.8.8.8",
		Database:     "warehouse",
		TenantID:     "tenant",
		ClientID:     "client",
		ClientSecret: "secret",
	})
	require.NoError(t, err)

	db, err := sqlconnect.NewDB("fabric", configJSON)
	require.NoError(t, err)
	require.NotNil(t, db.SqlDB())
	require.NoError(t, db.Close())
}
