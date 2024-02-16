package databricks

import (
	"context"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

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
