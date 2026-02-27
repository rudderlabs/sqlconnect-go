package redshift

import (
	"context"
	"regexp"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

var schemaDoesNotExistRegex = regexp.MustCompile(`schema "(.*)" does not exist`)

func (db *DB) ListSchemas(ctx context.Context) ([]sqlconnect.SchemaRef, error) {
	// If the list schemas query is executed while a schema is being deleted, the query will fail with a schema does not exist error.
	retryableError := func(err error) bool {
		return schemaDoesNotExistRegex.MatchString(err.Error())
	}
	return retryOperationWithData(ctx, func() ([]sqlconnect.SchemaRef, error) {
		schemas, err := db.DB.ListSchemas(ctx)
		if err != nil && !retryableError(err) {
			return nil, backoff.Permanent(err)
		}
		return schemas, err
	})
}

// ListSchemasInCatalog returns schemas for the given catalog (database).
func (db *DB) ListSchemasInCatalog(ctx context.Context, catalog sqlconnect.CatalogRef) ([]sqlconnect.SchemaRef, error) {
	retryableError := func(err error) bool {
		return schemaDoesNotExistRegex.MatchString(err.Error())
	}
	return retryOperationWithData(ctx, func() ([]sqlconnect.SchemaRef, error) {
		schemas, err := db.DB.ListSchemasInCatalog(ctx, catalog)
		if err != nil && !retryableError(err) {
			return nil, backoff.Permanent(err)
		}
		return schemas, err
	})
}

// retryOperationWithData retries the given operation with a constant backoff policy of 100ms for 10 times.
func retryOperationWithData[T any](ctx context.Context, o backoff.OperationWithData[T]) (T, error) {
	return backoff.RetryWithData(o, backoff.WithMaxRetries(backoff.WithContext(backoff.NewConstantBackOff(100*time.Millisecond), ctx), 10))
}
