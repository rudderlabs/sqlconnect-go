// Package sqlerr classifies the errors a sqlconnect DB returns, so that a
// caller can act on them without importing the warehouse drivers.
package sqlerr

import (
	"errors"
	"net/http"
	"regexp"
	"strings"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	"github.com/snowflakedb/gosnowflake"
	"github.com/trinodb/trino-go-client/trino"
	"google.golang.org/api/googleapi"

	redshiftdriver "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/redshift/driver"
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
	databricksTableNotFound   = "[TABLE_OR_VIEW_NOT_FOUND]"
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
	// The Data API relays the server message and carries no SQLSTATE.
	redshiftDataMissingObject = regexp.MustCompile(`^(?:ERROR: )?(?:relation|schema) "[^"]+" does not exist`)
)

// IsObjectNotFound reports whether err, returned by a sqlconnect DB, says that
// the table or view, schema or database a statement addressed does not exist.
// A missing column or function and a permission failure are not reported,
// except where the engine does not tell them apart: Snowflake answers "does not
// exist or not authorized" for both. ClickHouse errors carry their own codes and
// are not inspected here; see clickhousequery.Describe.
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
	if sfErr, ok := errors.AsType[*gosnowflake.SnowflakeError](err); ok {
		return sfErr.Number == snowflakeObjectNotFoundOrUnauthorized || sfErr.Number == snowflakeObjectNotFound
	}
	if gErr, ok := errors.AsType[*googleapi.Error](err); ok {
		return bigQueryObjectNotFound(gErr)
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
	return false
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
		return class == databricksSchemaNotFound
	case "":
		return class == databricksTableNotFound || class == databricksSchemaNotFound
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
