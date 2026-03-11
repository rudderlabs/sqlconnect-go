package trino

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

func isCatalogNotFoundError(err error) bool {
	var queryErr *trino.ErrQueryFailed

	fmt.Println("*** ERROR ***", err)
	if errors.As(err, &queryErr) {
		if queryErr.Reason != nil {
			reason := queryErr.Reason.Error()
			if strings.Contains(reason, "Catalog") && strings.Contains(reason, "not found") {
				return true
			}
		}
	}
	return false
}
