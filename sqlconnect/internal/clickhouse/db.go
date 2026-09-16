package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

const DatabaseType = "clickhouse"

type DB struct {
	*base.DB
	config Config
}

var _ sqlconnect.DB = (*DB)(nil)
var _ sqlconnect.ErrorClassifier = (*DB)(nil)

func NewDB(configJSON json.RawMessage) (*DB, error) {
	var config Config
	if err := config.Parse(configJSON); err != nil {
		return nil, err
	}
	pool := ch.OpenDB(config.options())
	return &DB{DB: base.NewDB(pool, func() error { return nil }, base.WithDialect(newDialect()), base.WithColumnTypeMapper(columnTypeMapper)), config: config}, nil
}

func defaultSettings() ch.Settings {
	return ch.Settings{
		"join_use_nulls": 1, "session_timezone": "UTC", "async_insert": 0,
		"wait_for_async_insert": 1, "select_sequential_consistency": 1,
	}
}

func queryContext(ctx context.Context) context.Context {
	overrides := sqlconnect.QuerySettings(ctx)
	// Preserve contexts built directly with the native driver. Connection options
	// supply defaults when the caller does not use the sqlconnect settings helper.
	if overrides == nil {
		return ctx
	}
	settings := defaultSettings()
	for key, value := range overrides {
		settings[key] = value
	}
	// WithSettings replaces its parent's map. Reassert correctness constraints in
	// the complete new map instead of mutating a context shared by other queries.
	settings["join_use_nulls"] = 1
	settings["async_insert"] = 0
	settings["wait_for_async_insert"] = 1
	settings["select_sequential_consistency"] = 1
	return ch.Context(ctx, ch.WithSettings(settings))
}

func (db *DB) Exec(query string, args ...any) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}
func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.SqlDB().ExecContext(queryContext(ctx), query, args...)
}
func (db *DB) Query(query string, args ...any) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}
func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return db.SqlDB().QueryContext(queryContext(ctx), query, args...)
}
func (db *DB) QueryRow(query string, args ...any) *sql.Row {
	return db.QueryRowContext(context.Background(), query, args...)
}
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return db.SqlDB().QueryRowContext(queryContext(ctx), query, args...)
}
func (db *DB) Ping() error { return db.PingContext(context.Background()) }
func (db *DB) PingContext(ctx context.Context) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx = queryContext(ctx)
	var version, database string
	if err := conn.QueryRowContext(ctx, "SELECT version(), currentDatabase()").Scan(&version, &database); err != nil {
		return err
	}
	supported, err := supportedVersion(version)
	if err != nil {
		return err
	}
	if !supported {
		return adapterError("CH_VERSION_BELOW_FLOOR", "requires ClickHouse >= 25.8.0", sqlconnect.ErrNotSupported)
	}
	if database != db.config.Database {
		return adapterError("CH_CONFIG_INVALID", "current database differs from configured database", nil)
	}
	rows, err := conn.QueryContext(ctx, `SELECT name, value FROM system.settings WHERE name IN ('join_use_nulls', 'async_insert', 'wait_for_async_insert', 'select_sequential_consistency', 'readonly', 'date_time_input_format', 'default_table_engine', 'session_timezone') ORDER BY name`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (db *DB) Begin() (*sql.Tx, error) { return db.BeginTx(context.Background(), nil) }
func (db *DB) BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error) {
	return nil, adapterError("CH_TRANSACTIONS_UNSUPPORTED", "ClickHouse has no supported transactions", sqlconnect.ErrNotSupported)
}
func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	return db.PrepareContext(context.Background(), query)
}
func (db *DB) PrepareContext(context.Context, string) (*sql.Stmt, error) {
	return nil, adapterError("CH_PREPARE_UNSUPPORTED", "ClickHouse prepares insert batches, not SQL statements", sqlconnect.ErrNotSupported)
}
func (db *DB) CurrentCatalog(context.Context) (sqlconnect.CatalogRef, error) {
	return sqlconnect.CatalogRef{}, sqlconnect.ErrNotSupported
}
func (db *DB) ListCatalogs(context.Context) ([]sqlconnect.CatalogRef, error) {
	return nil, sqlconnect.ErrNotSupported
}

func init() {
	sqlconnect.RegisterDBFactory(DatabaseType, func(raw json.RawMessage) (sqlconnect.DB, error) { return NewDB(raw) })
}
