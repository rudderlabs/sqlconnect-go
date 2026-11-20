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
		maxComputeCleanupConfig(),
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
			if c.skipped(configJSON) {
				log.Printf("[%s] skipped: %s is empty", c.Type, c.Env)
				return nil
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
					if c.dryRun(os.Getenv) {
						log.Printf("[%s] dry run: would drop schema %s (set %s=true to drop)", c.Type, schema, c.DropEnv)
						continue
					}
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
	// SkipIfEmpty skips the warehouse, rather than failing the whole run, when Env
	// is set but empty.
	SkipIfEmpty bool
	// DropEnv, when set, makes the sweep a dry run unless that variable is "true".
	DropEnv string
}

// maxComputeCleanupConfig is MaxCompute's entry. The dev project is shared with
// other teams' probes and DropSchema drops with CASCADE, so only this repo's
// exact test names older than 24 h are dropped; a run in progress is never
// touched. The workflow passes an empty value until the repository secret
// exists, and the sweep drops only when MAXCOMPUTE_CLEANUP_DROP=true; otherwise
// it logs.
func maxComputeCleanupConfig() cleanupConfig {
	return cleanupConfig{
		Env: "MAXCOMPUTE_TEST_ENVIRONMENT_CREDENTIALS", Type: maxcompute.DatabaseType,
		SkipIfEmpty: true, DropEnv: "MAXCOMPUTE_CLEANUP_DROP",
		Sweep: func(name string) bool { return maxComputeSweepable(name, time.Now()) },
	}
}

// skipped reports whether the warehouse is skipped for configJSON, the value of
// Env: empty or whitespace only.
func (c cleanupConfig) skipped(configJSON string) bool {
	return c.SkipIfEmpty && strings.TrimSpace(configJSON) == ""
}

// dryRun reports whether the sweep only logs: DropEnv is set and not "true".
func (c cleanupConfig) dryRun(getenv func(string) string) bool {
	return c.DropEnv != "" && getenv(c.DropEnv) != "true"
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
