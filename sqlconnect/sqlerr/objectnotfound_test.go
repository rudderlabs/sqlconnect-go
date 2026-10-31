package sqlerr_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/snowflakedb/gosnowflake/v2"
	"github.com/stretchr/testify/require"
	"github.com/trinodb/trino-go-client/trino"
	"google.golang.org/api/googleapi"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
	redshiftdriver "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/redshift/driver"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/sqlerr"
)

// Embeds the interface so only Error and SqlState need bodies.
type fakeDBExecErr struct {
	dbsqlerr.DBExecutionError
	state, msg string
}

func (f fakeDBExecErr) Error() string    { return f.msg }
func (f fakeDBExecErr) SqlState() string { return f.state }

func dataAPIFailure(message string) error {
	return &redshiftdriver.StatementError{Status: types.StatusStringFailed, Message: message}
}

func TestIsObjectNotFound(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("executing query: %w", err) }

	t.Run("a missing table, schema or database", func(t *testing.T) {
		cases := map[string]error{
			"postgres 42P01 undefined table":                &pq.Error{Code: "42P01", Message: `relation "s.t" does not exist`},
			"postgres 3F000 invalid schema name":            &pq.Error{Code: "3F000", Message: `schema "s" does not exist`},
			"postgres 3D000 invalid catalog name":           &pq.Error{Code: "3D000", Message: `database "d" does not exist`},
			"mysql 1146 no such table":                      &mysql.MySQLError{Number: 1146, Message: "Table 'd.t' doesn't exist"},
			"mysql 1049 unknown database":                   &mysql.MySQLError{Number: 1049, Message: "Unknown database 'd'"},
			"snowflake 2003 object":                         &gosnowflake.SnowflakeError{Number: 2003, SQLState: "42S02", Message: "SQL compilation error: Object 'T' does not exist or not authorized."},
			"snowflake 2003 schema":                         &gosnowflake.SnowflakeError{Number: 2003, SQLState: "02000", Message: "SQL compilation error: Schema 'D.S' does not exist or not authorized."},
			"snowflake 2043 object":                         &gosnowflake.SnowflakeError{Number: 2043, SQLState: "02000", Message: "SQL compilation error: Object does not exist, or operation cannot be performed."},
			"bigquery 404 table":                            &googleapi.Error{Code: 404, Message: "Not found: Table p:d.t", Errors: []googleapi.ErrorItem{{Reason: "notFound"}}},
			"bigquery 404 dataset":                          &googleapi.Error{Code: 404, Message: "Not found: Dataset p:d", Errors: []googleapi.ErrorItem{{Reason: "notFound"}}},
			"trino TABLE_NOT_FOUND":                         &trino.ErrQueryFailed{Reason: &trino.ErrTrino{ErrorName: "TABLE_NOT_FOUND", ErrorCode: 46, Message: "line 1:15: Table 'c.s.t' does not exist"}},
			"trino SCHEMA_NOT_FOUND":                        &trino.ErrQueryFailed{Reason: &trino.ErrTrino{ErrorName: "SCHEMA_NOT_FOUND", ErrorCode: 45, Message: "line 1:15: Schema 's' does not exist"}},
			"trino CATALOG_NOT_FOUND":                       &trino.ErrQueryFailed{Reason: &trino.ErrTrino{ErrorName: "CATALOG_NOT_FOUND", ErrorCode: 44, Message: "line 1:15: Catalog 'c' not found"}},
			"databricks 42P01 table":                        fakeDBExecErr{state: "42P01", msg: "databricks: execution error: [TABLE_OR_VIEW_NOT_FOUND] The table or view `t` cannot be found."},
			"databricks 42704 with the schema class":        fakeDBExecErr{state: "42704", msg: "databricks: execution error: [SCHEMA_NOT_FOUND] The schema `s` cannot be found."},
			"databricks synchronous rejection, table":       fakeDBExecErr{state: "", msg: "databricks: execution error: failed to execute query: [TABLE_OR_VIEW_NOT_FOUND] The table or view `t` cannot be found. SQLSTATE: 42P01"},
			"databricks synchronous rejection, schema":      fakeDBExecErr{state: "", msg: "databricks: execution error: failed to execute query: unexpected operation state ERROR_STATE: [SCHEMA_NOT_FOUND] The schema `s` cannot be found."},
			"redshift data api relation":                    dataAPIFailure(`ERROR: relation "s.t" does not exist`),
			"redshift data api schema":                      dataAPIFailure(`ERROR: schema "s" does not exist`),
			"redshift data api without the severity":        dataAPIFailure(`relation "s.t" does not exist`),
			"redshift data api quoted identifier":           dataAPIFailure(`ERROR: relation "s.a"b" does not exist`),
			"databricks 42704 with the catalog class":       fakeDBExecErr{state: "42704", msg: "databricks: execution error: [CATALOG_NOT_FOUND] The catalog `c` cannot be found."},
			"databricks synchronous rejection, catalog":     fakeDBExecErr{state: "", msg: "databricks: execution error: failed to execute query: [CATALOG_NOT_FOUND] The catalog `c` cannot be found."},
			"fabric 208 invalid object name":                mssql.Error{Number: 208, Message: "Invalid object name 's.t'."},
			"redshift data api identifier with a newline":   dataAPIFailure("ERROR: relation \"a\nb\" does not exist"),
			"bigquery job error table":                      &bigquery.Error{Reason: "notFound", Message: "Not found: Table p:d.t was not found in location US"},
			"fabric 208 followed by the termination notice": mssql.Error{Number: 3621, Message: "The statement has been terminated.", All: []mssql.Error{{Number: 208, Message: "Invalid object name 's.t'."}, {Number: 3621, Message: "The statement has been terminated."}}},
		}
		for name, err := range cases {
			t.Run(name, func(t *testing.T) {
				require.True(t, sqlerr.IsObjectNotFound(err))
				require.True(t, sqlerr.IsObjectNotFound(wrap(err)), "wrapped")
			})
		}
	})

	t.Run("anything else", func(t *testing.T) {
		cases := map[string]error{
			"plain text with the phrase":                                             errors.New(`pq: relation "t" does not exist`),
			"postgres 42501 permission denied":                                       &pq.Error{Code: "42501", Message: "permission denied for relation t"},
			"postgres 42703 undefined column":                                        &pq.Error{Code: "42703", Message: `column "c" does not exist`},
			"postgres 42883 undefined function":                                      &pq.Error{Code: "42883", Message: "function f(integer) does not exist"},
			"mysql 1045 access denied":                                               &mysql.MySQLError{Number: 1045, Message: "Access denied for user"},
			"mysql 1054 unknown column":                                              &mysql.MySQLError{Number: 1054, Message: "Unknown column 'c' in 'field list'"},
			"snowflake 1003 syntax error":                                            &gosnowflake.SnowflakeError{Number: 1003, SQLState: "42000", Message: "SQL compilation error: syntax error"},
			"snowflake 3001 insufficient privileges":                                 &gosnowflake.SnowflakeError{Number: 3001, SQLState: "42501", Message: "Insufficient privileges to operate on table 'T'"},
			"bigquery 403":                                                           &googleapi.Error{Code: 403, Message: "Access Denied: Table p:d.t", Errors: []googleapi.ErrorItem{{Reason: "accessDenied"}}},
			"bigquery 404 job":                                                       &googleapi.Error{Code: 404, Message: "Not found: Job p:US.j", Errors: []googleapi.ErrorItem{{Reason: "notFound"}}},
			"bigquery 404 without the item reason":                                   &googleapi.Error{Code: 404, Message: "Not found: Table p:d.t"},
			"trino COLUMN_NOT_FOUND":                                                 &trino.ErrQueryFailed{Reason: &trino.ErrTrino{ErrorName: "COLUMN_NOT_FOUND", ErrorCode: 47, Message: "line 1:8: Column 'c' cannot be resolved"}},
			"trino PERMISSION_DENIED":                                                &trino.ErrQueryFailed{Reason: &trino.ErrTrino{ErrorName: "PERMISSION_DENIED", ErrorCode: 4, Message: "Access Denied: Cannot select from table t"}},
			"trino failure wrapping a response body":                                 &trino.ErrQueryFailed{StatusCode: 404, Reason: errors.New("<!DOCTYPE html>Table 'x' does not exist")},
			"databricks 42704 for another undefined object":                          fakeDBExecErr{state: "42704", msg: "databricks: execution error: [UNRESOLVED_ROUTINE] Cannot resolve routine `f`"},
			"databricks 42501 quoting the class":                                     fakeDBExecErr{state: "42501", msg: "databricks: execution error: [INSUFFICIENT_PERMISSIONS] no SELECT [TABLE_OR_VIEW_NOT_FOUND]"},
			"databricks empty sqlstate, class inside an identifier":                  fakeDBExecErr{state: "", msg: "databricks: execution error: failed to execute query: [INSUFFICIENT_PERMISSIONS] User does not have SELECT on table `a.[SCHEMA_NOT_FOUND]`."},
			"databricks empty sqlstate, no class":                                    fakeDBExecErr{state: "", msg: "databricks: execution error: failed to execute query: request timed out"},
			"databricks empty sqlstate, text before the class":                       fakeDBExecErr{state: "", msg: "databricks: execution error: failed to execute query: permission denied for object: [SCHEMA_NOT_FOUND]"},
			"redshift data api permission denied":                                    dataAPIFailure("ERROR: permission denied for relation t"),
			"redshift data api column":                                               dataAPIFailure(`ERROR: column "c" does not exist`),
			"redshift data api phrase not at the start":                              dataAPIFailure(`ERROR: syntax error at or near "relation "t" does not exist"`),
			"redshift data api aborted":                                              &redshiftdriver.StatementError{Status: types.StatusStringAborted, Message: "Query cancelled by user"},
			"fabric 229 permission denied":                                           mssql.Error{Number: 229, Message: "The SELECT permission was denied on the object 't', database 'd', schema 's'."},
			"fabric 207 invalid column":                                              mssql.Error{Number: 207, Message: "Invalid column name 'c'."},
			"redshift data api already exists, quoted phrase":                        dataAPIFailure(`ERROR: relation "a" does not exist" already exists`),
			"bigquery job error access denied":                                       &bigquery.Error{Reason: "accessDenied", Message: "Access Denied: Table p:d.t"},
			"bigquery job error not found job":                                       &bigquery.Error{Reason: "notFound", Message: "Not found: Job p:US.j"},
			"fabric diagnostics without 208":                                         mssql.Error{Number: 3621, Message: "The statement has been terminated.", All: []mssql.Error{{Number: 229, Message: "The SELECT permission was denied on the object 't'."}, {Number: 3621, Message: "The statement has been terminated."}}},
			"redshift data api already exists, phrase and newline in the identifier": dataAPIFailure("ERROR: relation \"a\" does not exist\nb\" already exists"),
			"clickhouse keeps its own code":                                          cherr.New(cherr.CodeObjectNotFound, "", "the table or database does not exist"),
			"network":                                                                &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
			"deadline":                                                               context.DeadlineExceeded,
		}
		for name, err := range cases {
			t.Run(name, func(t *testing.T) {
				require.False(t, sqlerr.IsObjectNotFound(err))
				require.False(t, sqlerr.IsObjectNotFound(wrap(err)), "wrapped")
			})
		}
		require.False(t, sqlerr.IsObjectNotFound(nil))
	})
}
