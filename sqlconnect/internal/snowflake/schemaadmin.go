package snowflake

import (
	"context"
	"strconv"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/snowflakequery"
)

// SchemaExists overrides the base implementation to handle nonexistent catalog gracefully
func (db *DB) SchemaExists(ctx context.Context, schemaRef sqlconnect.SchemaRef, opts ...sqlconnect.Option) (bool, error) {
	exists, err := db.DB.SchemaExists(ctx, schemaRef, opts...)
	if err != nil {
		if isObjectInaccessibleError(err) {
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
		if isObjectInaccessibleError(err) {
			return []sqlconnect.SchemaRef{}, nil
		}
		return nil, err
	}
	return schemas, nil
}

const (
	sfErrObjectNotFound         = 2043
	sfErrInsufficientPrivileges = 2003
)

func classify(err error) sqlconnect.ErrorInfo {
	details, ok := snowflakequery.Describe(err)
	if !ok {
		return sqlconnect.ErrorInfo{}
	}
	info := sqlconnect.ErrorInfo{Code: strconv.Itoa(details.Number), ServerCode: int32(details.Number)}
	switch details.Number {
	case sfErrObjectNotFound:
		info.Category = "not_found"
	case sfErrInsufficientPrivileges:
		info.Category = "permission"
	}
	return info
}

func isObjectInaccessibleError(err error) bool {
	category := classify(err).Category
	return category == "not_found" || category == "permission"
}
