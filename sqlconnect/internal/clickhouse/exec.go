package clickhouse

import (
	"context"
	"database/sql"
	"math"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chctx"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

// driverExec runs the driver's own statements on one pinned connection. Each
// statement gets a fresh copy of the settings map and a fresh query id, so no
// caller map and no earlier id reach it.
type driverExec struct {
	conn     *sql.Conn
	settings func() map[string]any
}

var _ sqlconnect.QueryExecutor = driverExec{}

func (e driverExec) stmt(ctx context.Context) context.Context {
	return clickhousequery.WithStatement(ctx, e.settings(), clickhousequery.NewQueryID())
}

func (e driverExec) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return e.conn.ExecContext(e.stmt(ctx), q, args...)
}

func (e driverExec) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return e.conn.QueryContext(e.stmt(ctx), q, args...)
}

func (e driverExec) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	return e.conn.QueryRowContext(e.stmt(ctx), q, args...)
}

// withConn runs fn on one connection from the pool under the driver scratch
// map. Every driver write goes through it: database/sql replays a failed pool
// statement, and a replayed write could apply twice.
func (db *DB) withConn(ctx context.Context, fn func(ex driverExec) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return bound("connect", "", err)
	}
	defer func() { _ = conn.Close() }()
	return fn(driverExec{conn: conn, settings: driverScratchSettings})
}

// readCtx keeps a caller's statement map; otherwise it attaches the driver
// read map and a fresh query id.
func (db *DB) readCtx(ctx context.Context) context.Context {
	if chctx.Has(ctx) {
		return ctx
	}
	return clickhousequery.WithStatement(ctx, driverReadSettings(), clickhousequery.NewQueryID())
}

// resolve checks a relation reference before any SQL is built from it. It
// refuses a catalog, fills an omitted schema with the configured database and
// refuses empty or NUL-carrying components.
func (db *DB) resolve(ref sqlconnect.RelationRef) (sqlconnect.RelationRef, error) {
	if ref.Catalog != "" {
		return ref, catalogUnsupported()
	}
	if ref.Schema == "" {
		ref.Schema = db.cfg.Database
	}
	if err := checkComponent(ref.Schema); err != nil {
		return ref, err
	}
	return ref, checkComponent(ref.Name)
}

// resolveSchema is resolve for a database name.
func (db *DB) resolveSchema(schema sqlconnect.SchemaRef) (sqlconnect.SchemaRef, error) {
	if schema.Name == "" {
		schema.Name = db.cfg.Database
	}
	return schema, checkComponent(schema.Name)
}

// checkComponent refuses a name that no quoting can carry. QuoteIdentifier
// escapes backticks and backslashes, so only empty and NUL remain.
func checkComponent(name string) error {
	if name == "" || strings.ContainsRune(name, 0) {
		return cherr.New(cherr.CodeInvalidReference, "reference", fixedMessages[cherr.CodeInvalidReference])
	}
	return nil
}

func catalogUnsupported() error {
	return wrap(cherr.CodeCatalogUnsupported, "catalog", fixedMessages[cherr.CodeCatalogUnsupported], sqlconnect.ErrNotSupported)
}

// countToInt converts a server count to int, or returns CH_COUNT_OVERFLOW.
func countToInt(n uint64) (int, error) {
	if n > math.MaxInt {
		return 0, cherr.New(cherr.CodeCountOverflow, "", fixedMessages[cherr.CodeCountOverflow])
	}
	return int(n), nil
}

// queryCount runs a one-value count query on the pool with the read map.
func (db *DB) queryCount(ctx context.Context, op, q string, args ...any) (int, error) {
	var n uint64
	if err := db.QueryRowContext(db.readCtx(ctx), q, args...).Scan(&n); err != nil {
		return 0, bound(op, "", err)
	}
	return countToInt(n)
}
