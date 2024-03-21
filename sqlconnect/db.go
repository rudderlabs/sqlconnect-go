package sqlconnect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"
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

type Dialect interface {
	// QuoteTable quotes a table name
	QuoteTable(table RelationRef) string
	// QuoteIdentifier quotes an identifier, e.g. a column name
	QuoteIdentifier(name string) string
	// FormatTableName formats a table name, typically by lower or upper casing it, depending on the database
	FormatTableName(name string) string
}
