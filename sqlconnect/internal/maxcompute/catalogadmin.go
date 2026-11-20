package maxcompute

import (
	"context"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcsql"
)

// CurrentCatalog returns the configured project: a MaxCompute catalog is a project.
func (db *DB) CurrentCatalog(ctx context.Context) (sqlconnect.CatalogRef, error) {
	if err := ctx.Err(); err != nil {
		return sqlconnect.CatalogRef{}, mcerr.Context(err, "", "")
	}
	return sqlconnect.CatalogRef{Name: db.project()}, nil
}

// ListCatalogs is not supported: listing projects needs grants rETL does not
// ask for, and the model flow never calls it.
func (db *DB) ListCatalogs(context.Context) ([]sqlconnect.CatalogRef, error) {
	return nil, sqlconnect.ErrNotSupported
}

func (db *DB) project() string { return mcsql.Normalise(db.config.Project) }

// otherCatalog reports whether catalog names a project other than the
// configured one. The driver reads metadata of its own project only.
func (db *DB) otherCatalog(catalog string) bool {
	return catalog != "" && mcsql.Normalise(catalog) != db.project()
}
