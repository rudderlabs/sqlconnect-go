package maxcompute

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcsql"
)

// ListSchemas lists the project's schemas through the REST API: MaxCompute
// has no per-project information_schema in schema mode.
func (db *DB) ListSchemas(ctx context.Context, opts ...sqlconnect.Option) ([]sqlconnect.SchemaRef, error) {
	filter, err := sqlconnect.NewFilterOptions(opts...)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, mcerr.Context(err, "", "")
	}
	if db.otherCatalog(filter.Catalog) {
		return nil, nil
	}
	var schemas []sqlconnect.SchemaRef
	err = readCall(ctx, func() error {
		schemas = nil
		var listErr error
		if err := db.client.Schemas().List(func(s *odps.Schema, err error) {
			if err != nil {
				listErr = err
				return
			}
			schemas = append(schemas, sqlconnect.SchemaRef{Name: s.Name()})
		}); err != nil {
			return err
		}
		return listErr
	})
	if err != nil {
		return nil, wrap("listing schemas", err)
	}
	return schemas, nil
}

// SchemaExists loads the schema: 404 (0420111) means it does not exist.
func (db *DB) SchemaExists(ctx context.Context, schemaRef sqlconnect.SchemaRef, opts ...sqlconnect.Option) (bool, error) {
	filter, err := sqlconnect.NewFilterOptions(opts...)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, mcerr.Context(err, "", "")
	}
	if db.otherCatalog(filter.Catalog) {
		return false, nil
	}
	name := mcsql.Normalise(schemaRef.Name)
	if mcsql.CheckName(name) != nil {
		return false, nil //nolint:nilerr // a name MaxCompute cannot hold names no schema
	}
	err = readCall(ctx, func() error { return db.client.Schema(name).Load() })
	if err == nil {
		return true, nil
	}
	if httpStatus(err) == http.StatusNotFound {
		return false, nil
	}
	return false, wrap("checking schema "+name, err)
}

// CreateSchema creates the schema if it does not exist. rETL does not call
// it: the customer creates the working schema (HLD OQ9). A dropped and
// re-created schema name can make IF NOT EXISTS lie for minutes (HLD OQ8).
func (db *DB) CreateSchema(ctx context.Context, schema sqlconnect.SchemaRef) error {
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+db.QuoteIdentifier(mcsql.Normalise(schema.Name))); err != nil {
		return fmt.Errorf("creating schema %s: %w", schema, err)
	}
	return nil
}

// DropSchema drops the schema and everything in it. MaxCompute refuses to
// drop a schema that holds tables unless CASCADE is given. Tests only.
func (db *DB) DropSchema(ctx context.Context, schema sqlconnect.SchemaRef) error {
	if _, err := db.ExecContext(ctx, "DROP SCHEMA "+db.QuoteIdentifier(mcsql.Normalise(schema.Name))+" CASCADE"); err != nil {
		return fmt.Errorf("dropping schema %s: %w", schema, err)
	}
	return nil
}
