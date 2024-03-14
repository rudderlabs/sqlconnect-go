package base

import (
	"context"
	"fmt"
)

// CurrentCatalog returns the current catalog
func (db *DB) CurrentCatalog(ctx context.Context) (string, error) {
	var catalog string
	if err := db.QueryRowContext(ctx, db.sqlCommands.CurrentCatalog()).Scan(&catalog); err != nil {
		return "", fmt.Errorf("getting current catalog: %w", err)
	}
	return catalog, nil
}
