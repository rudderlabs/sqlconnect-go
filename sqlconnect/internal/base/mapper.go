package base

import (
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// JSONRowMapper returns a row mapper that maps scanned rows to [map[string]any]
func (db *DB) JSONRowMapper() sqlconnect.RowMapper[map[string]any] {
	return sqlconnect.JSONRowMapper(db.jsonRowMapper)
}
