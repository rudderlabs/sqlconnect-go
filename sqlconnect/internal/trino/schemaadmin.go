package trino

import (
	"context"
	"errors"

	"github.com/trinodb/trino-go-client/trino"

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

// trinoCatalogNotFoundErrorCode is the Trino error code for "catalog not found"
const trinoCatalogNotFoundErrorCode = 44

func isCatalogNotFoundError(err error) bool {
	var queryErr *trino.ErrQueryFailed
	if errors.As(err, &queryErr) {
		var trinoErr *trino.ErrTrino
		if errors.As(queryErr.Unwrap(), &trinoErr) {
			return trinoErr.ErrorCode == trinoCatalogNotFoundErrorCode
		}
	}
	return false
}
