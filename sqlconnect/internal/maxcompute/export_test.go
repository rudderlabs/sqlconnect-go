package maxcompute

import (
	"context"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// TableLifecycleForTest returns a table's lifecycle in days, read over REST.
func (db *DB) TableLifecycleForTest(ctx context.Context, relation sqlconnect.RelationRef) (int, error) {
	table, err := db.table(relation)
	if err != nil {
		return 0, err
	}
	if err := readCall(ctx, table.Load); err != nil {
		return 0, err
	}
	return table.LifeCycle(), nil
}
