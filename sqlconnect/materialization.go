package sqlconnect

import (
	"context"
	"database/sql"
)

// QueryExecutor is the statement surface shared by *sql.DB and *sql.Conn.
type QueryExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type MaterializationOptions struct {
	SortingKey        []string
	RequireSortingKey bool
	AllowNullableKey  bool
	Columns           []ColumnRef
	Visibility        VisibilityPolicy
}

// MaterializationAdmin is an optional interface for drivers whose create from
// query needs an explicit engine, sorting key or visibility check.
type MaterializationAdmin interface {
	// CreateTableForQueryWithOptions runs DESCRIBE, CREATE and AwaitTable.
	// It runs no INSERT.
	CreateTableForQueryWithOptions(
		context.Context, QueryExecutor, RelationRef, string, MaterializationOptions,
	) (uuid string, err error)
	// InsertFromQueryWithOptions runs the INSERT ... SELECT into a table
	// that CreateTableForQueryWithOptions created with the same options.
	InsertFromQueryWithOptions(
		context.Context, QueryExecutor, RelationRef, string, MaterializationOptions,
	) error
	// CreateTableFromQueryWithOptions runs both calls above in sequence.
	CreateTableFromQueryWithOptions(
		context.Context, QueryExecutor, RelationRef, string, MaterializationOptions,
	) (uuid string, err error)
	MoveTableWithOptions(
		context.Context, QueryExecutor, RelationRef, RelationRef, MaterializationOptions,
	) (uuid string, err error)
}
