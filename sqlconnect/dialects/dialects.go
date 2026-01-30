// Package dialects provides a constructor for creating SQL dialects without database connections.
// This is useful for SQL generation where you need proper identifier quoting and normalization.
//
// Example:
//
//	import "github.com/rudderlabs/sqlconnect-go/sqlconnect/dialects"
//
//	dialect, err := dialects.NewDialect("snowflake")
//	if err != nil {
//	    return err
//	}
//	normalized := dialect.NormaliseIdentifier("users")  // Returns "USERS"
//	quoted := dialect.QuoteIdentifier("user_id")        // Returns "user_id" (with proper quoting)
package dialects

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/bigquery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/databricks"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/mysql"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/postgres"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/redshift"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/snowflake"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/trino"
)

// NewDialect returns a Dialect for the specified warehouse type without requiring a DB connection.
// This is useful for SQL generation where you need proper identifier quoting and normalization.
//
// Supported warehouse types:
//   - "snowflake"
//   - "bigquery"
//   - "redshift"
//   - "postgres"
//   - "databricks"
//   - "mysql"
//   - "trino"
func NewDialect(warehouseType string) (sqlconnect.Dialect, error) {
	allowedWarehouseTypes := []string{"snowflake", "bigquery", "redshift", "postgres", "databricks", "mysql", "trino"}
	switch warehouseType {
	case "snowflake":
		return snowflake.NewDialect(), nil
	case "bigquery":
		return bigquery.NewDialect(), nil
	case "redshift":
		return redshift.NewDialect(), nil
	case "postgres":
		return postgres.NewDialect(), nil
	case "databricks":
		return databricks.NewDialect(), nil
	case "mysql":
		return mysql.NewDialect(), nil
	case "trino":
		return trino.NewDialect(), nil
	default:
		return nil, fmt.Errorf("unknown warehouse type: %s, allowed values are: %s", warehouseType, strings.Join(allowedWarehouseTypes, ", "))
	}
}

// NewRedshiftDialectWithOptions returns a Redshift dialect with configurable case sensitivity.
// Set caseSensitive to true if the Redshift cluster has enable_case_sensitive_identifier=on.
func NewRedshiftDialectWithOptions(caseSensitive bool) sqlconnect.Dialect {
	return redshift.NewDialectWithOptions(caseSensitive)
}
