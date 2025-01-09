package databricks

import (
	"context"
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// ListColumns returns a list of columns for the given table
func (db *DB) ListColumns(ctx context.Context, relation sqlconnect.RelationRef) ([]sqlconnect.ColumnRef, error) {
	if !db.informationSchema && relation.Catalog != "" {
		currentCatalog, err := db.CurrentCatalog(ctx) // make sure the catalog matches the current catalog
		if err != nil {
			return nil, fmt.Errorf("getting current catalog: %w", err)
		}
		if relation.Catalog != currentCatalog {
			return nil, fmt.Errorf("catalog %s not found", relation.Catalog)
		}
	}
	cols, err := db.DB.ListColumns(ctx, relation)
	if db.skipColumnNormalization {
		return cols, err
	}
	return lo.Map(cols, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
		col.Name = db.NormaliseIdentifier(col.Name)
		return col
	}), err
}

func (db *DB) ListColumnsForSqlQuery(ctx context.Context, sql string) ([]sqlconnect.ColumnRef, error) {
	cols, err := db.DB.ListColumnsForSqlQuery(ctx, sql)
	if db.skipColumnNormalization {
		return cols, err
	}
	return lo.Map(cols, func(col sqlconnect.ColumnRef, _ int) sqlconnect.ColumnRef {
		col.Name = db.NormaliseIdentifier(col.Name)
		return col
	}), err
}

// RenameTable in databricks falls back to MoveTable if rename is not supported
func (db *DB) RenameTable(ctx context.Context, oldRef, newRef sqlconnect.RelationRef) error {
	if err := db.DB.RenameTable(ctx, oldRef, newRef); err != nil {
		// move table if rename is not supported
		if strings.Contains(err.Error(), "DELTA_ALTER_TABLE_RENAME_NOT_ALLOWED") {
			return db.MoveTable(ctx, oldRef, newRef)
		}
		return err
	}
	return nil
}
