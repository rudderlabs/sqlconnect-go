package mysql

import (
	"context"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// CurrentCatalog returns an error because it is not supported by MySQL
func (db *DB) CurrentCatalog(ctx context.Context) (string, error) {
	return "", sqlconnect.ErrNotSupported
}
