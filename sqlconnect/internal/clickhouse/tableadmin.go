package clickhouse

import (
	"context"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func (db *DB) database(name string) string {
	if name == "" {
		return db.config.Database
	}
	return name
}
func (db *DB) relation(ref sqlconnect.RelationRef, write bool) (sqlconnect.RelationRef, error) {
	if ref.Catalog != "" {
		return ref, adapterError("CH_CATALOG_UNSUPPORTED", "ClickHouse has no catalog", sqlconnect.ErrNotSupported)
	}
	ref.Schema = db.database(ref.Schema)
	if ref.Name == "" || ref.Schema == "" || strings.ContainsRune(ref.Name+ref.Schema, 0) {
		return ref, adapterError("CH_INVALID_REFERENCE", "database and table names must be nonempty and contain no NUL", nil)
	}
	if write && ref.Schema != db.config.ScratchDatabase {
		return ref, adapterError("CH_CONFIG_INVALID", "writes require the configured scratchDatabase", nil)
	}
	return ref, nil
}

// Scratch databases are provisioned by the operator, never by the connector.
func (db *DB) CreateSchema(context.Context, sqlconnect.SchemaRef) error {
	return sqlconnect.ErrNotSupported
}
func (db *DB) DropSchema(context.Context, sqlconnect.SchemaRef) error {
	return sqlconnect.ErrNotSupported
}
func (db *DB) ListSchemas(ctx context.Context, opts ...sqlconnect.Option) ([]sqlconnect.SchemaRef, error) {
	options, err := sqlconnect.NewFilterOptions(opts...)
	if err != nil {
		return nil, err
	}
	result := []sqlconnect.SchemaRef{}
	if options.Catalog != "" {
		return result, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		result = append(result, sqlconnect.SchemaRef{Name: name})
	}
	return result, rows.Err()
}
func (db *DB) SchemaExists(ctx context.Context, ref sqlconnect.SchemaRef, opts ...sqlconnect.Option) (bool, error) {
	options, err := sqlconnect.NewFilterOptions(opts...)
	if err != nil {
		return false, err
	}
	if options.Catalog != "" {
		return false, nil
	}
	count, err := db.count(ctx, "SELECT count() FROM system.databases WHERE name = ?", db.database(ref.Name))
	return count > 0, err
}
func (db *DB) ListTables(ctx context.Context, schema sqlconnect.SchemaRef, opts ...sqlconnect.Option) ([]sqlconnect.RelationRef, error) {
	options, err := sqlconnect.NewTableListOptions(opts...)
	if err != nil {
		return nil, err
	}
	result := []sqlconnect.RelationRef{}
	if options.Catalog != "" {
		return result, nil
	}
	database := db.database(schema.Name)
	rows, err := db.QueryContext(ctx, `SELECT name, engine FROM system.tables WHERE database = ? AND is_temporary = 0 AND startsWith(name, ?) ORDER BY name`, database, options.Prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name, engine string
		if err := rows.Scan(&name, &engine); err != nil {
			return nil, err
		}
		kind := sqlconnect.TableRelation
		switch engine {
		case "View", "MaterializedView", "WindowView":
			kind = sqlconnect.ViewRelation
		}
		result = append(result, sqlconnect.RelationRef{Schema: database, Name: name, Type: kind})
	}
	return result, rows.Err()
}
func (db *DB) TableExists(ctx context.Context, ref sqlconnect.RelationRef) (bool, error) {
	if ref.Catalog != "" {
		return false, nil
	}
	ref, err := db.relation(ref, false)
	if err != nil {
		return false, err
	}
	count, err := db.count(ctx, `SELECT count() FROM system.tables WHERE database = ? AND name = ? AND is_temporary = 0`, ref.Schema, ref.Name)
	return count > 0, err
}
func (db *DB) ListColumns(ctx context.Context, ref sqlconnect.RelationRef) ([]sqlconnect.ColumnRef, error) {
	if ref.Catalog != "" {
		return []sqlconnect.ColumnRef{}, nil
	}
	ref, err := db.relation(ref, false)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT name, type FROM system.columns WHERE database = ? AND table = ? ORDER BY position`, ref.Schema, ref.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []sqlconnect.ColumnRef{}
	for rows.Next() {
		var name, raw string
		if err := rows.Scan(&name, &raw); err != nil {
			return nil, err
		}
		result = append(result, sqlconnect.ColumnRef{Name: name, RawType: raw, Type: canonicalType(raw)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, adapterError("CH_RELATION_NOT_FOUND", "relation is absent or invisible", nil)
	}
	return result, nil
}
func (db *DB) ListColumnsForSqlQuery(ctx context.Context, query string) ([]sqlconnect.ColumnRef, error) {
	query, err := selectQuery(query)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, "SELECT * FROM ("+query+") AS _sqlconnect_metadata LIMIT 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	result := make([]sqlconnect.ColumnRef, 0, len(columns))
	names := map[string]bool{}
	for _, column := range columns {
		if names[column.Name()] {
			return nil, adapterError("CH_DUPLICATE_COLUMN", "query contains duplicate column names", nil)
		}
		names[column.Name()] = true
		result = append(result, sqlconnect.ColumnRef{Name: column.Name(), RawType: column.DatabaseTypeName(), Type: columnTypeMapper(column)})
	}
	return result, rows.Err()
}
func (db *DB) count(ctx context.Context, query string, args ...any) (int, error) {
	var count uint64
	if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, err
	}
	if count > uint64(^uint(0)>>1) {
		return 0, adapterError("CH_COUNT_OVERFLOW", "row count exceeds platform int", nil)
	}
	return int(count), nil
}
func (db *DB) CountTableRows(ctx context.Context, ref sqlconnect.RelationRef) (int, error) {
	ref, err := db.relation(ref, false)
	if err != nil {
		return 0, err
	}
	return db.count(ctx, "SELECT count() FROM "+db.QuoteTable(ref))
}
func (db *DB) GetRowCountForQuery(ctx context.Context, query string, params ...any) (int, error) {
	query, err := selectQuery(query)
	if err != nil {
		return 0, err
	}
	return db.count(ctx, "SELECT count() FROM ("+query+") AS _sqlconnect_count", params...)
}
func (db *DB) tableDDL(ctx context.Context, ref sqlconnect.RelationRef, prefix, suffix string) error {
	ref, err := db.relation(ref, true)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, prefix+db.QuoteTable(ref)+suffix)
	return err
}
func (db *DB) CreateTestTable(ctx context.Context, ref sqlconnect.RelationRef) error {
	return db.tableDDL(ctx, ref, "CREATE TABLE IF NOT EXISTS ", " (c1 Int64, c2 String) ENGINE = MergeTree ORDER BY (c1)")
}
func (db *DB) DropTable(ctx context.Context, ref sqlconnect.RelationRef) error {
	return db.tableDDL(ctx, ref, "DROP TABLE IF EXISTS ", " SYNC")
}
func (db *DB) TruncateTable(ctx context.Context, ref sqlconnect.RelationRef) error {
	return db.tableDDL(ctx, ref, "TRUNCATE TABLE ", " SYNC")
}
func (db *DB) RenameTable(ctx context.Context, oldRef, newRef sqlconnect.RelationRef) error {
	oldRef, err := db.relation(oldRef, true)
	if err != nil {
		return err
	}
	newRef, err = db.relation(newRef, true)
	if err != nil {
		return err
	}
	if oldRef.Schema != newRef.Schema {
		return adapterError("CH_CROSS_DATABASE_MOVE_UNSUPPORTED", "cannot move across databases", sqlconnect.ErrNotSupported)
	}
	_, err = db.ExecContext(ctx, "RENAME TABLE "+db.QuoteTable(oldRef)+" TO "+db.QuoteTable(newRef))
	return err
}

// MoveTable uses a server-side rename. The destination must not exist. Snapshot
// publication that replaces an existing table must use EXCHANGE TABLES explicitly.
func (db *DB) MoveTable(ctx context.Context, oldRef, newRef sqlconnect.RelationRef) error {
	return db.RenameTable(ctx, oldRef, newRef)
}

// CreateTableFromQuery materializes values in two statements. ORDER BY tuple()
// supplies no useful sorting key; callers needing keyed snapshots must create
// their table explicitly. Failure may leave a partial target, never retried here.
func (db *DB) CreateTableFromQuery(ctx context.Context, ref sqlconnect.RelationRef, query string) error {
	ref, err := db.relation(ref, true)
	if err != nil {
		return err
	}
	query, err = selectQuery(query)
	if err != nil {
		return err
	}
	columns, err := db.ListColumnsForSqlQuery(ctx, query)
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return adapterError("CH_INVALID_QUERY", "query has no columns", nil)
	}
	definitions := make([]string, len(columns))
	names := make([]string, len(columns))
	for i, column := range columns {
		if column.Type == "unsupported" {
			return adapterError("CH_TYPE_UNSUPPORTED", "project unsupported columns before materialization", nil)
		}
		names[i] = db.QuoteIdentifier(column.Name)
		definitions[i] = names[i] + " " + column.RawType
	}
	table := db.QuoteTable(ref)
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+table+" ("+strings.Join(definitions, ", ")+") ENGINE = MergeTree ORDER BY tuple()"); err != nil {
		return err
	}
	projection := strings.Join(names, ", ")
	_, err = db.ExecContext(ctx, "INSERT INTO "+table+" ("+projection+") SELECT "+projection+" FROM ("+query+") AS _sqlconnect_source")
	return err
}
