package maxcompute_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
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

// runID is unique per test run. Every object these tests create in the shared
// project carries the prefix rs_retl_probe_sc_<runID>_. A schema name is never
// reused: a re-created schema can make IF EXISTS and even a CTAS lie for
// minutes (HLD OQ8).
var runID = func() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}()

func objectName(name string) string { return "rs_retl_probe_sc_" + runID + "_" + name }

// testSchema creates a fresh schema for the test and drops it, with
// everything in it, when the test ends. The name holds tsqlcon_ and its unix
// time, so the scheduled sweeper (sqlconnect/cmd/cleanup) drops a schema a
// killed run leaked once it is a day old.
func testSchema(t *testing.T, db *maxcompute.DB, name string) string {
	t.Helper()
	schema := objectName(fmt.Sprintf("tsqlcon_%s_%d", name, time.Now().Unix()))
	ctx := context.Background()
	require.NoError(t, db.CreateSchema(ctx, sqlconnect.SchemaRef{Name: schema}))
	t.Cleanup(func() {
		if err := db.DropSchema(context.Background(), sqlconnect.SchemaRef{Name: schema}); err != nil {
			t.Logf("dropping schema %s: %v", schema, err)
		}
	})
	return schema
}

// slowQuery runs for about 35 seconds on the dev project (36M rows took 19 s)
// and reads no table. split drops trailing empty strings, as Java's does, so
// the generator splits repeat('a,', n) rather than space(n).
const slowQuery = "SELECT /*+ MAPJOIN(y) */ SUM(LENGTH(MD5(CONCAT(CAST(x.pos AS STRING), CAST(y.pos AS STRING))))) FROM " +
	"(SELECT pos FROM (SELECT posexplode(split(repeat('a,', 8000), ',')) AS (pos, v)) g) x CROSS JOIN " +
	"(SELECT pos FROM (SELECT posexplode(split(repeat('a,', 8000), ',')) AS (pos, v)) g) y"
