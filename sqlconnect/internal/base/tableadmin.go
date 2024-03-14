package base

import (
	"context"
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// CreateTestTable creates a test table
func (db *DB) CreateTestTable(ctx context.Context, table sqlconnect.RelationRef) error {
	_, err := db.ExecContext(ctx, db.sqlCommands.CreateTestTable(QuotedIdentifier(db.QuoteTable(table))))
	return err
}

// ListTables returns a list of tables in the given schema
func (db *DB) ListTables(ctx context.Context, schema sqlconnect.SchemaRef) ([]sqlconnect.RelationRef, error) {
	var res []sqlconnect.RelationRef
	for _, tuple := range db.sqlCommands.ListTables(UnquotedIdentifier(schema.Name)) {
		stmt := tuple.A
		colName := tuple.B
		rows, err := db.QueryContext(ctx, stmt)
		if err != nil {
			return nil, fmt.Errorf("querying list tables for schema %s: %w", schema, err)
		}
		defer func() { _ = rows.Close() }()
		cols, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("getting columns in list tables for schema %s: %w", schema, err)
		}
		cols = lo.Map(cols, func(col string, _ int) string { return strings.ToLower(col) })
		var name string
		scanValues := make([]any, len(cols))
		if len(cols) == 1 {
			scanValues[0] = &name
		} else {
			tableNameColIdx := lo.IndexOf(cols, strings.ToLower(colName))
			if tableNameColIdx == -1 {
				return nil, fmt.Errorf("column %s not found in result set: %+v", colName, cols)
			}
			var otherCol sqlconnect.NilAny
			for i := 0; i < len(cols); i++ {
				if i == tableNameColIdx {
					scanValues[i] = &name
				} else {
					scanValues[i] = &otherCol
				}
			}
		}
		for rows.Next() {
			err = rows.Scan(scanValues...)
			if err != nil {
				return nil, fmt.Errorf("scanning list tables: %w", err)
			}
			res = append(res, sqlconnect.NewRelationRef(name, sqlconnect.WithSchema(schema.Name)))
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterating list tables: %w", err)
		}
	}

	return res, nil
}

// ListTablesWithPrefix returns a list of tables in the given schema that have the given prefix
func (db *DB) ListTablesWithPrefix(ctx context.Context, schema sqlconnect.SchemaRef, prefix string) ([]sqlconnect.RelationRef, error) {
	var res []sqlconnect.RelationRef
	for _, tuple := range db.sqlCommands.ListTablesWithPrefix(UnquotedIdentifier(schema.Name), prefix) {
		stmt := tuple.A
		colName := tuple.B
		rows, err := db.QueryContext(ctx, stmt)
		if err != nil {
			return nil, fmt.Errorf("querying list tables for schema %s with prefix %s: %w", schema, prefix, err)
		}
		defer func() { _ = rows.Close() }()
		cols, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("getting columns in list tables for schema %s with prefix %s: %w", schema, prefix, err)
		}
		cols = lo.Map(cols, func(col string, _ int) string { return strings.ToLower(col) })
		var name string
		scanValues := make([]any, len(cols))
		if len(cols) == 1 {
			scanValues[0] = &name
		} else {
			tableNameColIdx := lo.IndexOf(cols, strings.ToLower(colName))
			if tableNameColIdx == -1 {
				return nil, fmt.Errorf("column %s not found in result set: %+v", colName, cols)
			}
			var otherCol sqlconnect.NilAny
			for i := 0; i < len(cols); i++ {
				if i == tableNameColIdx {
					scanValues[i] = &name
				} else {
					scanValues[i] = &otherCol
				}
			}
		}
		for rows.Next() {
			if err := rows.Scan(scanValues...); err != nil {
				return nil, fmt.Errorf("scanning list tables for schema %s with prefix %s: %w", schema, prefix, err)
			}
			res = append(res, sqlconnect.NewRelationRef(name, sqlconnect.WithSchema(schema.Name)))
		}
		// rows.Err will report the last error encountered by rows.Scan.
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterating list tables for schema %s with prefix %s: %w", schema, prefix, err)
		}
	}
	return res, nil
}

// TableExists returns true if the table exists
func (db *DB) TableExists(ctx context.Context, relation sqlconnect.RelationRef) (bool, error) {
	stmt := db.sqlCommands.TableExists(UnquotedIdentifier(relation.Schema), UnquotedIdentifier(relation.Name))
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return false, fmt.Errorf("querying table %s exists: %w", relation, err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		return true, nil
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterating table %s exists: %w", relation, err)
	}
	return false, nil
}

// ListColumns returns a list of columns for the given table
func (db *DB) ListColumns(ctx context.Context, relation sqlconnect.RelationRef) ([]sqlconnect.ColumnRef, error) {
	var res []sqlconnect.ColumnRef
	stmt, nameCol, typeCol := db.sqlCommands.ListColumns(UnquotedIdentifier(relation.Catalog), UnquotedIdentifier(relation.Schema), UnquotedIdentifier(relation.Name))
	columns, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("querying list columns for %s: %w", relation.String(), err)
	}
	defer func() { _ = columns.Close() }()
	cols, err := columns.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting columns in list columns for %s: %w", relation.String(), err)
	}
	cols = lo.Map(cols, func(col string, _ int) string { return strings.ToLower(col) })

	var column sqlconnect.ColumnRef
	scanValues := make([]any, len(cols))
	nameColIdx := lo.IndexOf(cols, strings.ToLower(nameCol))
	if nameColIdx == -1 {
		return nil, fmt.Errorf("column %s not found in result set: %+v", nameCol, cols)
	}
	typeColIdx := lo.IndexOf(cols, strings.ToLower(typeCol))
	if typeColIdx == -1 {
		return nil, fmt.Errorf("column %s not found in result set: %+v", typeCol, cols)
	}
	var otherCol sqlconnect.NilAny
	for i := 0; i < len(cols); i++ {
		if i == nameColIdx {
			scanValues[i] = &column.Name
		} else if i == typeColIdx {
			scanValues[i] = &column.RawType
		} else {
			scanValues[i] = &otherCol
		}
	}

	for columns.Next() {
		if err := columns.Scan(scanValues...); err != nil {
			return nil, fmt.Errorf("scanning list columns for %s: %w", relation.String(), err)
		}
		column.Type = db.columnTypeMapper(colRefTypeAdapter{column})
		res = append(res, column)
	}

	if err := columns.Err(); err != nil {
		return nil, fmt.Errorf("iterating list columns for %s: %w", relation.String(), err)
	}
	return res, nil
}

// ListColumnsForSqlQuery returns a list of columns for the given sql query
func (db *DB) ListColumnsForSqlQuery(ctx context.Context, sql string) ([]sqlconnect.ColumnRef, error) {
	var res []sqlconnect.ColumnRef
	rows, err := db.DB.QueryContext(ctx, sql) // nolint:rowserrcheck
	if err != nil {
		return nil, fmt.Errorf("querying list columns for sql query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("getting column information in list columns for sql query: %w", err)
	}
	for _, col := range colTypes {
		res = append(res, sqlconnect.ColumnRef{
			Name:    col.Name(),
			Type:    db.columnTypeMapper(col),
			RawType: col.DatabaseTypeName(),
		})
	}
	return res, nil
}

// CountTableRows returns the number of rows in the given table
func (c *DB) CountTableRows(ctx context.Context, relation sqlconnect.RelationRef) (int, error) {
	var count int
	if err := c.QueryRowContext(ctx, c.sqlCommands.CountTableRows(QuotedIdentifier(c.QuoteTable(relation)))).Scan(&count); err != nil {
		return 0, fmt.Errorf("counting table rows for %s: %w", relation.String(), err)
	}
	return count, nil
}

// DropTable drops a table
func (db *DB) DropTable(ctx context.Context, ref sqlconnect.RelationRef) error {
	if _, err := db.ExecContext(ctx, db.sqlCommands.DropTable(QuotedIdentifier(db.QuoteTable(ref)))); err != nil {
		return fmt.Errorf("dropping table %s: %w", ref.String(), err)
	}
	return nil
}

// TruncateTable truncates a table
func (db *DB) TruncateTable(ctx context.Context, ref sqlconnect.RelationRef) error {
	if _, err := db.ExecContext(ctx, db.sqlCommands.TruncateTable(QuotedIdentifier(db.QuoteTable(ref)))); err != nil {
		return fmt.Errorf("truncating table %s: %w", ref.String(), err)
	}
	return nil
}

// RenameTable renames a table
func (db *DB) RenameTable(ctx context.Context, oldRef, newRef sqlconnect.RelationRef) error {
	if oldRef.Schema != newRef.Schema {
		return fmt.Errorf("moving table to another schema not supported, oldRef: %s newRef: %s", oldRef, newRef)
	}
	if _, err := db.ExecContext(ctx, db.sqlCommands.RenameTable(QuotedIdentifier(db.QuoteIdentifier(oldRef.Schema)), QuotedIdentifier(db.QuoteIdentifier(oldRef.Name)), QuotedIdentifier(db.QuoteIdentifier(newRef.Name)))); err != nil {
		return fmt.Errorf("renaming table %s to %s: %w", oldRef.String(), newRef.String(), err)
	}
	return nil
}

// MoveTable copies the old table's contents to the new table and drops the old table. Returns [ErrDropOldTablePostCopy] if the old table could not be dropped after the copy.
func (db *DB) MoveTable(ctx context.Context, oldRef, newRef sqlconnect.RelationRef) error {
	if oldRef.Schema != newRef.Schema {
		return fmt.Errorf("moving table to another schema not supported, oldRef: %s newRef: %s", oldRef, newRef)
	}
	if _, err := db.ExecContext(ctx, db.sqlCommands.MoveTable(QuotedIdentifier(db.QuoteIdentifier(oldRef.Schema)), QuotedIdentifier(db.QuoteIdentifier(oldRef.Name)), QuotedIdentifier(db.QuoteIdentifier(newRef.Name)))); err != nil {
		return fmt.Errorf("copying table %s contents to %s: %w", oldRef.String(), newRef.String(), err)
	}
	if err := db.DropTable(ctx, oldRef); err != nil {
		return sqlconnect.ErrDropOldTablePostCopy
	}
	return nil
}

// CreateTableFromQuery creates a table from the results of a query
func (db *DB) CreateTableFromQuery(ctx context.Context, table sqlconnect.RelationRef, query string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %[1]s as (%[2]s)`, db.QuoteTable(table), query))
	return err
}

// GetRowCountForQuery returns the number of rows returned by the query
func (db *DB) GetRowCountForQuery(ctx context.Context, query string, params ...any) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, query, params...).Scan(&count)
	return count, err
}
