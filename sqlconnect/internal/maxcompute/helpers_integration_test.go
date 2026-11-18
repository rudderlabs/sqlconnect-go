package maxcompute_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute"
)

const credentialsEnv = "MAXCOMPUTE_TEST_ENVIRONMENT_CREDENTIALS"

// testConfig returns the dev project account, or skips the test when it is
// not set (fails under FORCE_RUN_INTEGRATION_TESTS).
func testConfig(t *testing.T) []byte {
	t.Helper()
	configJSON, ok := os.LookupEnv(credentialsEnv)
	if !ok {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatal(credentialsEnv + " environment variable not set")
		}
		t.Skip("skipping maxcompute integration test due to lack of a test environment")
	}
	return []byte(configJSON)
}

func testDB(t *testing.T) *maxcompute.DB {
	t.Helper()
	db, err := maxcompute.NewDB(testConfig(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// slowQuery runs for about 35 seconds on the dev project (36M rows took 19 s)
// and reads no table. split drops trailing empty strings, as Java's does, so
// the generator splits repeat('a,', n) rather than space(n).
const slowQuery = "SELECT /*+ MAPJOIN(y) */ SUM(LENGTH(MD5(CONCAT(CAST(x.pos AS STRING), CAST(y.pos AS STRING))))) FROM " +
	"(SELECT pos FROM (SELECT posexplode(split(repeat('a,', 8000), ',')) AS (pos, v)) g) x CROSS JOIN " +
	"(SELECT pos FROM (SELECT posexplode(split(repeat('a,', 8000), ',')) AS (pos, v)) g) y"
