package main

import (
	"context"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/sjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/bigquery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/databricks"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/fabric"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/redshift"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/snowflake"
)

func main() {
	cleanupConfigs := []cleanupConfig{
		{Env: "BIGQUERY_TEST_ENVIRONMENT_CREDENTIALS", Type: bigquery.DatabaseType},
		{Env: "DATABRICKS_TEST_ENVIRONMENT_CREDENTIALS", Type: databricks.DatabaseType, Fn: func(s string) string {
			s, _ = sjson.Set(s, "catalog", "hive_metastore")
			return s
		}},
		{Env: "DATABRICKS_TEST_ENVIRONMENT_CREDENTIALS", Type: databricks.DatabaseType, Fn: func(s string) string {
			s, _ = sjson.Set(s, "catalog", "sqlconnect")
			return s
		}},
		{Env: "FABRIC_TEST_ENVIRONMENT_CREDENTIALS", Type: fabric.DatabaseType, Fn: func(s string) string {
			s, _ = sjson.Delete(s, "fabricWorkspaceId")
			return s
		}},
		// MaxCompute: the dev project is shared with other teams' probes and
		// DropSchema drops with CASCADE, so only this repo's exact test names
		// older than 24 h are dropped; a run in progress is never touched.
		{Env: "MAXCOMPUTE_TEST_ENVIRONMENT_CREDENTIALS", Type: maxcompute.DatabaseType, Sweep: func(name string) bool {
			return maxComputeSweepable(name, time.Now())
		}},
		{Env: "REDSHIFT_DATA_TEST_ENVIRONMENT_CREDENTIALS", Type: redshift.DatabaseType},
		{Env: "REDSHIFT_TEST_ENVIRONMENT_CREDENTIALS", Type: redshift.DatabaseType},
		{Env: "SNOWFLAKE_TEST_ENVIRONMENT_CREDENTIALS", Type: snowflake.DatabaseType},
		// {Env: "TRINO_TEST_ENVIRONMENT_CREDENTIALS", Type: trino.DatabaseType},
	}

	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(4)
	for _, c := range cleanupConfigs {
		g.Go(func() error {
			configJSON, ok := os.LookupEnv(c.Env)
			if !ok {
				log.Fatalf("%s environment variable not set", c.Env)
			}
			if c.Fn != nil {
				configJSON = c.Fn(configJSON)
			}
			db, err := sqlconnect.NewDB(c.Type, []byte(configJSON))
			if err != nil {
				log.Fatalf("[%s] failed to create db: %v", c.Type, err)
			}
			schemas, err := db.ListSchemas(ctx)
			if err != nil {
				log.Fatalf("[%s] failed to list schemas: %v", c.Type, err)
			}
			sweep := c.Sweep
			if sweep == nil {
				sweep = func(name string) bool { return strings.Contains(strings.ToLower(name), "tsqlcon_") }
			}
			for _, schema := range schemas {
				if sweep(schema.Name) {
					err := db.DropSchema(ctx, schema)
					if err != nil {
						log.Printf("[%s] failed to drop schema: %v", c.Type, err)
					} else {
						log.Printf("[%s] dropped schema %s", c.Type, schema)
					}
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		log.Fatalf("cleanup failed: %v", err)
	}
}

type cleanupConfig struct {
	Type string
	Env  string
	Fn   func(string) string
	// Sweep reports whether a schema may be dropped. Nil means any schema
	// whose name contains tsqlcon_.
	Sweep func(name string) bool
}

// maxComputeTestSchema matches the schemas the MaxCompute tests create, and
// nothing else: rs_retl_probe_sc_<run>_tsqlcon_<name>_<unix>, from the shared
// suite and the package's own tests alike. A bare tsqlcon_ name is not
// attributable to this repo in a shared project, so it is never matched. The
// last group is the unix time the name was made.
var maxComputeTestSchema = regexp.MustCompile(`^rs_retl_probe_sc_[0-9a-f]{8}_tsqlcon_[a-z0-9_]+_([0-9]{10})$`)

// maxComputeSweepAge is the age below which a test schema may still be in use.
const maxComputeSweepAge = 24 * time.Hour

// maxComputeSweepable reports whether name is a MaxCompute test schema made
// more than 24 hours before now.
func maxComputeSweepable(name string, now time.Time) bool {
	m := maxComputeTestSchema.FindStringSubmatch(name)
	if m == nil {
		return false
	}
	created, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return false
	}
	return now.Sub(time.Unix(created, 0)) > maxComputeSweepAge
}
