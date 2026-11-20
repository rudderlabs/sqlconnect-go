package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/config"
)

// The config import alone must register the MaxCompute factory.
func TestMaxComputeConfigImportRegisters(t *testing.T) {
	cfg := []byte(`{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"LTAI0000","accessKeySecret":"s3cret"}`)
	db, err := sqlconnect.NewDB("maxcompute", cfg)
	require.NoError(t, err, "NewDB makes no network call")
	require.NoError(t, db.Close())
	_ = config.MaxCompute{}
}
