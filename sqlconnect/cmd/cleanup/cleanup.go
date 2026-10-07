package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/tidwall/sjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/bigquery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/databricks"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/fabric"
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
		{Env: "REDSHIFT_DATA_TEST_ENVIRONMENT_CREDENTIALS", Type: redshift.DatabaseType},
		{Env: "REDSHIFT_TEST_ENVIRONMENT_CREDENTIALS", Type: redshift.DatabaseType},
		{Env: "SNOWFLAKE_TEST_ENVIRONMENT_CREDENTIALS", Type: snowflake.DatabaseType},
		// {Env: "TRINO_TEST_ENVIRONMENT_CREDENTIALS", Type: trino.DatabaseType},
	}

	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(4)
	for _, c := range cleanupConfigs {
		g.Go(func() error {
			configJSON, ok := cleanupConfigFromEnv(c)
			if !ok {
				log.Printf("[%s] skipping cleanup: %s environment variable not set or empty", c.Type, c.Env)
				return nil
			}
			db, err := sqlconnect.NewDB(c.Type, []byte(configJSON))
			if err != nil {
				log.Fatalf("[%s] failed to create db: %v", c.Type, err)
			}
			schemas, err := db.ListSchemas(ctx)
			if err != nil {
				log.Fatalf("[%s] failed to list schemas: %v", c.Type, err)
			}
			for _, schema := range schemas {
				if strings.Contains(strings.ToLower(schema.Name), "tsqlcon_") {
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
}

func cleanupConfigFromEnv(config cleanupConfig) (string, bool) {
	configJSON := strings.TrimSpace(os.Getenv(config.Env))
	if configJSON == "" {
		return "", false
	}
	if config.Fn != nil {
		configJSON = config.Fn(configJSON)
	}
	return configJSON, true
}
