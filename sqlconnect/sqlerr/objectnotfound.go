// Package sqlerr classifies the errors a sqlconnect DB returns, so that a
// caller can act on them without importing the warehouse drivers.
package sqlerr

import (
	"errors"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/bigquery"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/trinodb/trino-go-client/trino"
	"google.golang.org/api/googleapi"

	redshiftdriver "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/redshift/driver"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/snowflakequery"
)

const (
	pqUndefinedTable     = "42P01"
	pqInvalidSchemaName  = "3F000"
	pqInvalidCatalogName = "3D000"

	mysqlNoSuchTable     = 1146
	mysqlUnknownDatabase = 1049

	// "does not exist or not authorized": Snowflake answers the same for both.
	snowflakeObjectNotFoundOrUnauthorized = 2003
	snowflakeObjectNotFound               = 2043

	databricksUndefinedTable  = "42P01"
	databricksUndefinedObject = "42704"
	databricksSchemaNotFound  = "[SCHEMA_NOT_FOUND]"
	databricksCatalogNotFound = "[CATALOG_NOT_FOUND]"
	databricksTableNotFound   = "[TABLE_OR_VIEW_NOT_FOUND]"

	// "Invalid object name": SQL Server answers the same for an object the login cannot see.
	mssqlInvalidObjectName = 208
)

var (
	trinoMissingObjectNames = map[string]bool{"TABLE_NOT_FOUND": true, "SCHEMA_NOT_FOUND": true, "CATALOG_NOT_FOUND": true}
	bigQueryMissingPrefixes = []string{"Not found: Table", "Not found: Dataset"}
	// What databricks-sql-go prepends to the server message, whose first token is its error class.
	databricksDriverPrefixes = []string{
		"databricks: execution error: ",
		"failed to execute query: ",
		"unexpected operation state ERROR_STATE: ",
	}
	// The Data API relays the server message and carries no SQLSTATE. The identifier
	// is printed as is, so it may itself contain a double quote or a newline; the
	// phrase has to end the message.
	redshiftDataMissingObject = regexp.MustCompile(`(?s)^(?:ERROR: )?(?:relation|schema) ".+" does not exist\n?$`)
)

// IsObjectNotFound reports whether err, returned by a sqlconnect DB, says that
// the table or view, schema, database or catalog a statement addressed does not
// exist. A missing column or function and a permission failure are not
// reported, except where the engine does not tell them apart: Snowflake answers
// "does not exist or not authorized" (2003) and SQL Server "Invalid object name"
// (208) for an object the role cannot see as well. ClickHouse errors carry their
// own codes and are not inspected here; see clickhousequery.Describe.
func IsObjectNotFound(err error) bool {
	if pqErr, ok := errors.AsType[*pq.Error](err); ok {
		switch pqErr.Code {
		case pqUndefinedTable, pqInvalidSchemaName, pqInvalidCatalogName:
			return true
		}
		return false
	}
	if myErr, ok := errors.AsType[*mysql.MySQLError](err); ok {
		return myErr.Number == mysqlNoSuchTable || myErr.Number == mysqlUnknownDatabase
	}
	if details, ok := snowflakequery.Describe(err); ok {
		return details.Number == snowflakeObjectNotFoundOrUnauthorized || details.Number == snowflakeObjectNotFound
	}
	if gErr, ok := errors.AsType[*googleapi.Error](err); ok {
		return bigQueryObjectNotFound(gErr)
	}
	if jobErr, ok := errors.AsType[*bigquery.Error](err); ok {
		return bigQueryJobObjectNotFound(jobErr)
	}
	if jobErr, ok := errors.AsType[bigquery.Error](err); ok {
		return bigQueryJobObjectNotFound(&jobErr)
	}
	if multi, ok := errors.AsType[bigquery.MultiError](err); ok {
		return slices.ContainsFunc(multi, IsObjectNotFound)
	}
	if tErr, ok := errors.AsType[*trino.ErrTrino](err); ok {
		return trinoMissingObjectNames[tErr.ErrorName]
	}
	if dbErr, ok := errors.AsType[dbsqlerr.DBExecutionError](err); ok {
		return databricksObjectNotFound(dbErr)
	}
	if stmtErr, ok := errors.AsType[*redshiftdriver.StatementError](err); ok {
		return redshiftDataMissingObject.MatchString(stmtErr.Message)
	}
	if msErr, ok := errors.AsType[mssql.Error](err); ok {
		return mssqlObjectNotFound(msErr)
	}
	return false
}

// SQL Server can answer one statement with several diagnostics; go-mssqldb keeps
// the last in Number and all of them in All.
func mssqlObjectNotFound(msErr mssql.Error) bool {
	if msErr.Number == mssqlInvalidObjectName {
		return true
	}
	for _, diagnostic := range msErr.All {
		if diagnostic.Number == mssqlInvalidObjectName {
			return true
		}
	}
	return false
}

// A failed job is also reported through its status, as *bigquery.Error or a MultiError of them.
func bigQueryJobObjectNotFound(jobErr *bigquery.Error) bool {
	return jobErr.Reason == "notFound" && hasAnyPrefix(jobErr.Message, bigQueryMissingPrefixes)
}

func bigQueryObjectNotFound(gErr *googleapi.Error) bool {
	if gErr.Code != http.StatusNotFound {
		return false
	}
	notFound := false
	for _, item := range gErr.Errors {
		if item.Reason == "notFound" {
			notFound = true
			break
		}
	}
	return notFound && hasAnyPrefix(gErr.Message, bigQueryMissingPrefixes)
}

// 42P01 is undefined_table on its own. 42704 is any undefined object and a
// synchronous rejection has no SQLSTATE at all, so both need the error class
// the server puts at the start of its message.
func databricksObjectNotFound(dbErr dbsqlerr.DBExecutionError) bool {
	class := databricksErrorClass(dbErr.Error())
	switch dbErr.SqlState() {
	case databricksUndefinedTable:
		return true
	case databricksUndefinedObject:
		return class == databricksSchemaNotFound || class == databricksCatalogNotFound
	case "":
		return class == databricksTableNotFound || class == databricksSchemaNotFound || class == databricksCatalogNotFound
	}
	return false
}

func databricksErrorClass(msg string) string {
	for stripped := true; stripped; {
		stripped = false
		for _, prefix := range databricksDriverPrefixes {
			if rest, ok := strings.CutPrefix(msg, prefix); ok {
				msg, stripped = rest, true
			}
		}
	}
	if !strings.HasPrefix(msg, "[") {
		return ""
	}
	end := strings.IndexByte(msg, ']')
	if end < 0 {
		return ""
	}
	return msg[:end+1]
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}
