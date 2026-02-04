package base

import (
	"context"
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// CurrentCatalog returns the current catalog
func (db *DB) CurrentCatalog(ctx context.Context) (string, error) {
	var catalog string
	if err := db.QueryRowContext(ctx, db.sqlCommands.CurrentCatalog()).Scan(&catalog); err != nil {
		return "", fmt.Errorf("getting current catalog: %w", err)
	}
	return catalog, nil
}

// ListCatalogs returns a list of catalogs
func (db *DB) ListCatalogs(ctx context.Context) ([]sqlconnect.CatalogRef, error) {
	var res []sqlconnect.CatalogRef
	stmt, colName := db.sqlCommands.ListCatalogs()
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("querying list catalogs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting columns in list catalogs: %w", err)
	}
	cols = lo.Map(cols, func(col string, _ int) string { return strings.ToLower(col) })
	var catalog sqlconnect.CatalogRef
	scanValues := make([]any, len(cols))
	if len(cols) == 1 {
		scanValues[0] = &catalog.Name
	} else {
		catalogNameColIdx := lo.IndexOf(cols, strings.ToLower(colName))
		if catalogNameColIdx == -1 {
			return nil, fmt.Errorf("column %s not found in result set: %+v", colName, cols)
		}
		var otherCol sqlconnect.NilAny
		for i := 0; i < len(cols); i++ {
			if i == catalogNameColIdx {
				scanValues[i] = &catalog.Name
			} else {
				scanValues[i] = &otherCol
			}
		}
	}
	for rows.Next() {
		err = rows.Scan(scanValues...)
		if err != nil {
			return nil, fmt.Errorf("scanning list catalogs: %w", err)
		}
		res = append(res, catalog)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating list catalogs: %w", err)
	}
	return res, nil
}
