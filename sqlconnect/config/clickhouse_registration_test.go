package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/config"
)

// The config import alone must register the ClickHouse factories.
func TestSQ1_ConfigImportRegisters(t *testing.T) {
	require.NoError(t, clickhousequery.SetDialPolicy(clickhousequery.DialPolicy{}))
	cfg := []byte(`{"host":"ch.example.com","database":"analytics","user":"rudder_retl","password":"s3cret","secure":true}`)
	db, err := sqlconnect.NewDB("clickhouse", cfg)
	require.NoError(t, err, "NewDB stays lazy after config parsing")
	_, ok := db.(sqlconnect.ErrorClassifier)
	require.True(t, ok, "the ClickHouse DB classifies its errors")
	require.NoError(t, db.Close())
	_, err = sqlconnect.NewDialect("clickhouse", nil)
	require.NoError(t, err, "the standalone dialect needs no credentials")
	_ = config.ClickHouse{}
}
