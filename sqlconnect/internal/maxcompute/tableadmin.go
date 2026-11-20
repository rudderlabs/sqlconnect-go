package maxcompute

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcsql"
)

// ListTables lists the schema's tables and views through the REST API, with
// a literal name prefix (_ is not a wildcard). A listing does not carry the
// table type, so views come from a second listing filtered by type.
func (db *DB) ListTables(ctx context.Context, schema sqlconnect.SchemaRef, opts ...sqlconnect.Option) ([]sqlconnect.RelationRef, error) {
	listOpts, err := sqlconnect.NewTableListOptions(opts...)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, mcerr.Context(err, "", "")
	}
	if db.otherCatalog(listOpts.Catalog) {
		return nil, nil
	}
	name := mcsql.Normalise(schema.Name)
	if err := mcsql.CheckName(name); err != nil {
		return nil, mcerr.Local(mcerr.CategoryRefused, err.Error())
	}
	var filters []odps.TFilterFunc
	if listOpts.Prefix != "" {
		filters = append(filters, odps.TableFilter.NamePrefix(mcsql.Normalise(listOpts.Prefix)))
	}
	names, err := db.listTableNames(ctx, name, filters...)
	if err != nil {
		if exists, existsErr := db.SchemaExists(ctx, schema); existsErr == nil && !exists {
			return nil, mcerr.Local(mcerr.CategoryNotFound, "schema "+name+" not found")
		}
		return nil, wrap("listing tables in "+name, err)
	}
	views := map[string]bool{}
	for _, viewType := range []odps.TableType{odps.VirtualView, odps.MaterializedView} {
		viewNames, err := db.listTableNames(ctx, name, append(filters, odps.TableFilter.Type(viewType))...)
		if err != nil {
			return nil, wrap("listing views in "+name, err)
		}
		for _, v := range viewNames {
			views[v] = true
		}
	}
	refs := make([]sqlconnect.RelationRef, 0, len(names))
	for _, n := range names {
		refOpts := []sqlconnect.Option{sqlconnect.WithSchema(schema.Name)}
		if listOpts.Catalog != "" {
			refOpts = append(refOpts, sqlconnect.WithCatalog(listOpts.Catalog))
		}
		if views[n] {
			refOpts = append(refOpts, sqlconnect.WithRelationType(sqlconnect.ViewRelation))
		}
		refs = append(refs, sqlconnect.NewRelationRef(n, refOpts...))
	}
	return refs, nil
}

func (db *DB) listTableNames(ctx context.Context, schema string, filters ...odps.TFilterFunc) ([]string, error) {
	var names []string
	err := readCall(ctx, func() error {
		names = nil
		var listErr error
		odps.NewTables(db.client, db.config.Project, schema).List(func(t *odps.Table, err error) {
			if err != nil {
				listErr = err
				return
			}
			names = append(names, t.Name())
		}, filters...)
		return listErr
	})
	return names, err
}

// TableExists loads the table; a missing table is false, not an error.
func (db *DB) TableExists(ctx context.Context, relation sqlconnect.RelationRef) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, mcerr.Context(err, "", "")
	}
	if db.otherCatalog(relation.Catalog) {
		return false, nil
	}
	table, err := db.table(relation)
	if err != nil {
		return false, nil //nolint:nilerr // a name MaxCompute cannot hold names no table
	}
	err = readCall(ctx, table.Load)
	if err == nil {
		return true, nil
	}
	if httpStatus(err) == http.StatusNotFound {
		return false, nil
	}
	return false, wrap("checking table "+relation.String(), err)
}

// ListColumns loads the table's schema, partition columns included.
func (db *DB) ListColumns(ctx context.Context, relation sqlconnect.RelationRef) ([]sqlconnect.ColumnRef, error) {
	table, err := db.loadTable(ctx, relation, "listing columns of ")
	if err != nil {
		return nil, err
	}
	s := table.Schema()
	cols := make([]sqlconnect.ColumnRef, 0, len(s.Columns)+len(s.PartitionColumns))
	for _, c := range append(s.Columns, s.PartitionColumns...) {
		raw := c.Type.Name()
		cols = append(cols, sqlconnect.ColumnRef{Name: c.Name, Type: canonicalType(raw), RawType: raw})
	}
	return cols, nil
}

// ListPartitionColumns returns the names of the table's partition columns, in
// declaration order, or none for an unpartitioned table. ListColumns lists them
// among the plain columns, so a caller that must tell them apart uses this.
func (db *DB) ListPartitionColumns(ctx context.Context, relation sqlconnect.RelationRef) ([]string, error) {
	table, err := db.loadTable(ctx, relation, "listing partition columns of ")
	if err != nil {
		return nil, err
	}
	pcs := table.Schema().PartitionColumns
	names := make([]string, 0, len(pcs))
	for _, c := range pcs {
		names = append(names, c.Name)
	}
	return names, nil
}

// loadTable loads relation's SDK table with its schema. A missing table is a
// not_found error; any other failure is wrapped with what, the action.
func (db *DB) loadTable(ctx context.Context, relation sqlconnect.RelationRef, what string) (*odps.Table, error) {
	if err := ctx.Err(); err != nil {
		return nil, mcerr.Context(err, "", "")
	}
	if db.otherCatalog(relation.Catalog) {
		return nil, mcerr.Local(mcerr.CategoryNotFound, "relation "+relation.String()+" not found in this project")
	}
	table, err := db.table(relation)
	if err != nil {
		return nil, err
	}
	if err := readCall(ctx, table.Load); err != nil {
		if httpStatus(err) == http.StatusNotFound {
			return nil, mcerr.Local(mcerr.CategoryNotFound, "relation "+relation.String()+" not found")
		}
		return nil, wrap(what+relation.String(), err)
	}
	return table, nil
}

// table returns the SDK table of relation in the configured project.
func (db *DB) table(relation sqlconnect.RelationRef) (*odps.Table, error) {
	schema, name := mcsql.Normalise(relation.Schema), mcsql.Normalise(relation.Name)
	if mcsql.CheckName(name) != nil || (schema != "" && mcsql.CheckName(schema) != nil) {
		return nil, mcerr.Local(mcerr.CategoryRefused, "invalid relation name "+relation.String())
	}
	return odps.NewTables(db.client, db.config.Project, schema).Get(name), nil
}

// ListColumnsForSqlQuery reads the column types of the query's result. The
// query runs, so a query without a top-level LIMIT is wrapped with LIMIT 0;
// rudder-sources already sends its own LIMIT 1.
func (db *DB) ListColumnsForSqlQuery(ctx context.Context, sql string) ([]sqlconnect.ColumnRef, error) {
	limited, err := mcsql.LimitZero(sql)
	if err != nil {
		return nil, mcerr.LocalWrap(mcerr.CategorySyntax, "the query cannot be scanned: "+err.Error(), err)
	}
	return db.DB.ListColumnsForSqlQuery(ctx, limited)
}

// RenameTable renames within one schema. MaxCompute accepts only a bare new
// name after RENAME TO.
func (db *DB) RenameTable(ctx context.Context, oldRef, newRef sqlconnect.RelationRef) error {
	if mcsql.Normalise(oldRef.Schema) != mcsql.Normalise(newRef.Schema) {
		return fmt.Errorf("moving table to another schema not supported, oldRef: %s newRef: %s", oldRef, newRef)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", db.QuoteTable(oldRef), db.QuoteIdentifier(mcsql.Normalise(newRef.Name)))); err != nil {
		return fmt.Errorf("renaming table %s to %s: %w", oldRef, newRef, err)
	}
	return nil
}

// MoveTable copies the table with a CTAS and drops the old one. The copy gets
// the lifecycle of maxcomputequery.WithLifecycle when ctx has one.
func (db *DB) MoveTable(ctx context.Context, oldRef, newRef sqlconnect.RelationRef) error {
	if mcsql.Normalise(oldRef.Schema) != mcsql.Normalise(newRef.Schema) {
		return fmt.Errorf("moving table to another schema not supported, oldRef: %s newRef: %s", oldRef, newRef)
	}
	if _, err := db.ExecContext(ctx, ctasSQL(db.QuoteTable(newRef), lifecycleFrom(ctx), "SELECT * FROM "+db.QuoteTable(oldRef))); err != nil {
		return fmt.Errorf("copying table %s contents to %s: %w", oldRef, newRef, err)
	}
	if err := db.DropTable(ctx, oldRef); err != nil {
		return sqlconnect.ErrDropOldTablePostCopy
	}
	return nil
}

// CreateTableFromQuery creates a table with a CTAS. MaxCompute does not
// accept the parenthesised AS (…) form for every query, so none is added. The
// table gets the lifecycle of maxcomputequery.WithLifecycle when ctx has one;
// a project with odps.table.lifecycle=mandatory refuses a table without one.
func (db *DB) CreateTableFromQuery(ctx context.Context, table sqlconnect.RelationRef, query string) error {
	_, err := db.ExecContext(ctx, ctasSQL(db.QuoteTable(table), lifecycleFrom(ctx), query))
	return err
}

// ctasSQL is a CREATE TABLE … AS. The LIFECYCLE clause, when days > 0, goes
// before AS, the only place MaxCompute accepts it in a CTAS (findings 18).
func ctasSQL(table string, days int, query string) string {
	if days > 0 {
		return fmt.Sprintf("CREATE TABLE %s LIFECYCLE %d AS %s", table, days, query)
	}
	return fmt.Sprintf("CREATE TABLE %s AS %s", table, query)
}

// httpStatus returns the HTTP status of an SDK REST error, or 0.
func httpStatus(err error) int {
	if e, ok := errors.AsType[restclient.HttpError](err); ok {
		return e.StatusCode
	}
	if e, ok := errors.AsType[*restclient.HttpError](err); ok && e != nil {
		return e.StatusCode
	}
	return 0
}

// wrap classifies an SDK error and names the operation.
func wrap(op string, err error) error {
	e := mcerr.FromSDK(err)
	if e.Message != "" {
		e.Message = op + ": " + e.Message
	}
	return e
}

// readRetries bounds the retries of a read-only REST call after a transport
// error: a metadata read is idempotent, unlike a job submit.
const readRetries = 3

// readCall is call with retries of transport errors, for read-only REST calls.
func readCall(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; ; attempt++ {
		err = call(ctx, fn)
		if err == nil || attempt == readRetries || ctx.Err() != nil || !mcerr.IsTransport(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 300 * time.Millisecond):
		}
	}
}

// call runs an SDK call that takes no context and returns early when ctx ends.
func call(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
