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
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/redshift"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/snowflake"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/trino"
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
		{Env: "REDSHIFT_DATA_TEST_ENVIRONMENT_CREDENTIALS", Type: redshift.DatabaseType},
		{Env: "REDSHIFT_TEST_ENVIRONMENT_CREDENTIALS", Type: redshift.DatabaseType},
		{Env: "SNOWFLAKE_TEST_ENVIRONMENT_CREDENTIALS", Type: snowflake.DatabaseType},
		{Env: "TRINO_TEST_ENVIRONMENT_CREDENTIALS", Type: trino.DatabaseType},
	}

	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(4)
	for _, c := range cleanupConfigs {
		c := c
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
