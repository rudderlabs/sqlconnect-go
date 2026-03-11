package databricks

import (
	"context"
	"errors"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// SchemaExists overrides the base implementation to handle nonexistent catalog gracefully
func (db *DB) SchemaExists(ctx context.Context, schemaRef sqlconnect.SchemaRef, opts ...sqlconnect.Option) (bool, error) {
	exists, err := db.DB.SchemaExists(ctx, schemaRef, opts...)
	if err != nil {
		if isCatalogNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return exists, nil
}

// ListSchemas overrides the base implementation to handle nonexistent catalog gracefully
func (db *DB) ListSchemas(ctx context.Context, opts ...sqlconnect.Option) ([]sqlconnect.SchemaRef, error) {
	schemas, err := db.DB.ListSchemas(ctx, opts...)
	if err != nil {
		if isCatalogNotFoundError(err) {
			return []sqlconnect.SchemaRef{}, nil
		}
		return nil, err
	}
	return schemas, nil
}

func isCatalogNotFoundError(err error) bool {
	if execErr, ok := errors.AsType[dbsqlerr.DBExecutionError](err); ok {
		switch execErr.SqlState() {
		case "42501", // invalid permission to access catalog
			"42704": // catalog not found
			return true
		}
	}
	return false
}
