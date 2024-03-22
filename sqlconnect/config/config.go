package config

import (
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/bigquery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/databricks"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/mysql"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/postgres"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/redshift"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/snowflake"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/trino"
)

type (
	BigQuery     = bigquery.Config
	Databricks   = databricks.Config
	Mysql        = mysql.Config
	Postgres     = postgres.Config
	Redshift     = redshift.PostgresConfig
	RedshiftData = redshift.Config
	Snowflake    = snowflake.Config
	Trino        = trino.Config
)
