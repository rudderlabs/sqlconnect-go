package sqlconnect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/rudderlabs/goqu/v10"
)

var (
	ErrNotSupported         = errors.New("sqconnect: feature not supported")
	ErrDropOldTablePostCopy = errors.New("sqlconnect move table: dropping old table after copying its contents to the new table")
)

type DB interface {
	sqlDB
	// SqlDB returns the underlying *sql.DB
	SqlDB() *sql.DB
	CatalogAdmin
	SchemaAdmin
	TableAdmin
	JsonRowMapper
	Dialect
}

type sqlDB interface {
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
	Conn(ctx context.Context) (*sql.Conn, error)
	Driver() driver.Driver
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Ping() error
	PingContext(ctx context.Context) error
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	SetConnMaxIdleTime(d time.Duration)
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() sql.DBStats
}

type CatalogAdmin interface {
	// CurrentCatalog returns the current catalog.
	// If this operation is not supported by the warehouse [ErrNotSupported] will be returned.
	CurrentCatalog(ctx context.Context) (string, error)
	// ListCatalogs returns all available catalogs linked to the credentials.
	// Returns an empty list if no catalogs are accessible or if catalogs are not supported.
	// System catalogs are filtered out.
	ListCatalogs(ctx context.Context) ([]CatalogRef, error)
}

type SchemaAdmin interface {
	// CreateSchema creates a schema
	CreateSchema(ctx context.Context, schema SchemaRef) error
	// GetSchemas returns a list of schemas
	ListSchemas(ctx context.Context) ([]SchemaRef, error)
	// SchemaExists returns true if the schema exists
	SchemaExists(ctx context.Context, schemaRef SchemaRef) (bool, error)
	// DropSchema drops a schema
	DropSchema(ctx context.Context, schema SchemaRef) error
}

type TableAdmin interface {
	// CreateTestTable creates a test table
	CreateTestTable(ctx context.Context, relation RelationRef) error
	// ListTables returns a list of tables in the given schema
	ListTables(ctx context.Context, schema SchemaRef) ([]RelationRef, error)
	// ListTablesWithPrefix returns a list of tables in the given schema that have the given prefix
	ListTablesWithPrefix(ctx context.Context, schema SchemaRef, prefix string) ([]RelationRef, error)
	// TableExists returns true if the table exists
	TableExists(ctx context.Context, relation RelationRef) (bool, error)
	// ListColumns returns a list of columns for the given table
	ListColumns(ctx context.Context, relation RelationRef) ([]ColumnRef, error)
	// ListColumnsForSqlQuery returns a list of columns for the given sql query
	ListColumnsForSqlQuery(ctx context.Context, sql string) ([]ColumnRef, error)
	// CountTableRows returns the number of rows in the given table
	CountTableRows(ctx context.Context, table RelationRef) (count int, err error)
	// DropTable drops a table
	DropTable(ctx context.Context, ref RelationRef) error
	// TruncateTable truncates a table
	TruncateTable(ctx context.Context, ref RelationRef) error
	// RenameTable renames a table. It might fall back to using MoveTable if the underlying database does not support renaming tables.
	RenameTable(ctx context.Context, oldRef, newRef RelationRef) error
	// MoveTable creates a new table by copying the old table's contents to it and then drops the old table. Returns [ErrDropOldTablePostCopy] if the old table could not be dropped after copy.
	MoveTable(ctx context.Context, oldRef, newRef RelationRef) error
	// CreateTableFromQuery creates a table from the results of a query
	CreateTableFromQuery(ctx context.Context, table RelationRef, query string) error
	// GetRowCountForQuery returns the number of rows returned by the query
	GetRowCountForQuery(ctx context.Context, query string, params ...any) (int, error)
}

type JsonRowMapper interface {
	// JSONRowMapper returns a row mapper that maps rows to map[string]any
	JSONRowMapper() RowMapper[map[string]any]
}

type (
	Dialect interface {
		// QuoteTable quotes a table name
		QuoteTable(table RelationRef) string

		// QuoteIdentifier quotes an identifier, e.g. a column name
		QuoteIdentifier(name string) string

		// FormatTableName formats a table name, typically by lower or upper casing it, depending on the database
		//
		// Deprecated: to be removed in future versions, since its behaviour is not consistent across databases, e.g. using lowercase for BigQuery while it shouldn't.
		// If you want to have a consistent behaviour across databases, use [NormaliseIdentifier] and [ParseRelationRef] instead.
		FormatTableName(name string) string

		// NormaliseIdentifier normalises the identifier's parts that are unquoted, typically by lower or upper casing them, depending on the database
		NormaliseIdentifier(identifier string) string

		// ParseRelationRef parses a string into a RelationRef after normalising the identifier and stripping out surrounding quotes.
		// The result is a RelationRef with case-sensitive fields, i.e. it can be safely quoted (see [QuoteTable] and, for instance, used for matching against the database's information schema.
		ParseRelationRef(identifier string) (RelationRef, error)

		// QueryCondition returns a dialect-specific query expression for the provided identifier, operator and value(s).
		//
		// E.g. QueryCondition("age", "gt", 18) returns "age > 18"
		//
		// Each operator has a different number of arguments, e.g. [eq] requires one argument, [in] requires at least one argument, etc.
		// See [op] package for the list of supported operators
		QueryCondition(identifier, operator string, args ...any) (Expression, error)

		// ParseGoquExpression converts a goqu Expression to an Expression
		ParseGoquExpression(goquExpression GoquExpression) (Expression, error)

		// Expressions returns the dialect-specific expressions
		Expressions() Expressions
	}

	// GoquExpression represents a goqu expression
	GoquExpression = goqu.Expression

	// Expressions provides dialect-specific expressions
	Expressions interface {
		// TimestampAdd returns an expression that adds the interval to the timestamp value.
		// The value can either be a string literal (column, timestamp, function etc.) or a [time.Time] value.
		TimestampAdd(timeValue any, interval int, unit string) (Expression, error)

		// DateAdd returns an expression that adds the interval to the date value.
		// The value can either be a string literal (column, timestamp, function etc.) or a [time.Time] value.
		// Values are cast to [DATE].
		DateAdd(dateValue any, interval int, unit string) (Expression, error)

		// Literal creates a literal sql expression
		Literal(sql string, args ...any) (Expression, error)
	}

	// Expression represents a dialect-specific expression.
	// One can get the expression's SQL string by calling [String()] on it.
	Expression interface {
		GoquExpression() GoquExpression
		fmt.Stringer
	}
)
