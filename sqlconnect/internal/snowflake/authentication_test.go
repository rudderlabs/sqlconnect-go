package snowflake_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/snowflake"
)

func TestSnowflakeAuthentication(t *testing.T) {
	t.Run("key pair with passphrase", func(t *testing.T) {
		configJSON, ok := os.LookupEnv("SNOWFLAKE_TEST_AUTH_KEYPAIR_ENCRYPTED_CREDENTIALS")
		if !ok {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatal("SNOWFLAKE_TEST_AUTH_KEYPAIR_ENCRYPTED_CREDENTIALS environment variable not set")
			}
			t.Skip("skipping test due to lack of a test environment")
		}

		db, err := sqlconnect.NewDB(snowflake.DatabaseType, []byte(configJSON))
		require.NoError(t, err, "it should be able to create a new DB")
		defer func() { _ = db.Close() }()
		require.NoError(t, db.Ping(), "it should be able to ping the database")
	})
	t.Run("key pair without passphrase", func(t *testing.T) {
		configJSON, ok := os.LookupEnv("SNOWFLAKE_TEST_AUTH_KEYPAIR_UNENCRYPTED_CREDENTIALS")
		if !ok {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatal("SNOWFLAKE_TEST_AUTH_KEYPAIR_UNENCRYPTED_CREDENTIALS environment variable not set")
			}
			t.Skip("skipping test due to lack of a test environment")
		}
		db, err := sqlconnect.NewDB(snowflake.DatabaseType, []byte(configJSON))
		require.NoError(t, err, "it should be able to create a new DB")
		defer func() { _ = db.Close() }()
		require.NoError(t, db.Ping(), "it should be able to ping the database")
	})
}
