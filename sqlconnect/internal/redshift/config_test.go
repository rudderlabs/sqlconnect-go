package redshift_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/redshift"
)

func TestRedshiftConfig(t *testing.T) {
	// Create a new redshift config
	config := redshift.Config{
		ClusterIdentifier: "cluster-identifier",
		Database:          "database",
		User:              "user",
		Region:            "region",
		AccessKeyID:       "access-key-id",
		SecretAccessKey:   "secret",
		SessionToken:      "token",
	}
	configJSON, err := json.Marshal(&config)
	require.NoError(t, err)
	require.Equal(t, redshift.RedshiftDataConfigType, gjson.GetBytes(configJSON, "type").String())

	// Unmarshal the JSON back into a new SDKConfig
	var newConfig redshift.Config
	err = newConfig.Parse(configJSON)
	require.NoError(t, err)
	require.Equal(t, config, newConfig)
}
