package clickhouse

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chctx"
)

// guardConn wraps every connection the pool opens. It covers the raw pool
// API, a pinned *sql.Conn and the driver's own statements alike.
type guardConn struct{ inner driver.Conn }

var (
	_ driver.Conn               = (*guardConn)(nil)
	_ driver.ConnBeginTx        = (*guardConn)(nil)
	_ driver.ConnPrepareContext = (*guardConn)(nil)
	_ driver.ExecerContext      = (*guardConn)(nil)
	_ driver.QueryerContext     = (*guardConn)(nil)
	_ driver.Pinger             = (*guardConn)(nil)
	_ driver.SessionResetter    = (*guardConn)(nil)
	_ driver.NamedValueChecker  = (*guardConn)(nil)

	_ driver.Rows                           = (*guardRows)(nil)
	_ driver.RowsColumnTypeScanType         = (*guardRows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*guardRows)(nil)
	_ driver.RowsColumnTypeNullable         = (*guardRows)(nil)
	_ driver.RowsColumnTypePrecisionScale   = (*guardRows)(nil)
)

// statementCtx gives an unmarked context the driver map and a fresh query id.
// A marked context carries the caller's own map and passes unchanged.
func statementCtx(ctx context.Context, settings func() map[string]any) context.Context {
	if chctx.Has(ctx) {
		return ctx
	}
	return clickhousequery.WithStatement(ctx, settings(), clickhousequery.NewQueryID())
}

// sanitize bounds err. driver.ErrBadConn stays bare: it carries no server
// text, and database/sql drops the connection on it.
func sanitize(op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, driver.ErrBadConn):
		return driver.ErrBadConn
	}
	return bound(op, "", err)
}

func (c *guardConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.inner.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	r, err := execer.ExecContext(statementCtx(ctx, driverScratchSettings), q, args)
	if err != nil {
		return nil, sanitize("exec", err)
	}
	return r, nil
}

func (c *guardConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.inner.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	rows, err := queryer.QueryContext(statementCtx(ctx, driverReadSettings), q, args)
	if err != nil {
		return nil, sanitize("query", err)
	}
	return &guardRows{inner: rows}, nil
}

func (c *guardConn) Ping(ctx context.Context) error {
	p, ok := c.inner.(driver.Pinger)
	if !ok {
		return nil
	}
	return sanitize("ping", p.Ping(statementCtx(ctx, controlSettings)))
}

func (c *guardConn) Prepare(string) (driver.Stmt, error) { return nil, prepUnsupported() }

func (c *guardConn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return nil, prepUnsupported()
}

func (c *guardConn) Begin() (driver.Tx, error) { return nil, txUnsupported() }

func (c *guardConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return nil, txUnsupported()
}

func (c *guardConn) Close() error { return sanitize("close", c.inner.Close()) }

func (c *guardConn) ResetSession(ctx context.Context) error {
	if r, ok := c.inner.(driver.SessionResetter); ok {
		return sanitize("reset session", r.ResetSession(ctx))
	}
	return nil
}

func (c *guardConn) CheckNamedValue(nv *driver.NamedValue) error {
	if n, ok := c.inner.(driver.NamedValueChecker); ok {
		return n.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

// guardRows bounds errors that arrive while rows stream, such as a
// cancellation (394) or a timeout (159) mid-stream.
type guardRows struct{ inner driver.Rows }

func (r *guardRows) Columns() []string { return r.inner.Columns() }

func (r *guardRows) Close() error { return sanitize("close rows", r.inner.Close()) }

func (r *guardRows) Next(dest []driver.Value) error {
	err := r.inner.Next(dest)
	if err == io.EOF { // database/sql compares io.EOF by identity
		return err
	}
	return sanitize("read rows", err)
}

func (r *guardRows) ColumnTypeScanType(i int) reflect.Type {
	if t, ok := r.inner.(driver.RowsColumnTypeScanType); ok {
		return t.ColumnTypeScanType(i)
	}
	return reflect.TypeFor[any]()
}

func (r *guardRows) ColumnTypeDatabaseTypeName(i int) string {
	if t, ok := r.inner.(driver.RowsColumnTypeDatabaseTypeName); ok {
		return t.ColumnTypeDatabaseTypeName(i)
	}
	return ""
}

func (r *guardRows) ColumnTypeNullable(i int) (nullable, ok bool) {
	if t, isT := r.inner.(driver.RowsColumnTypeNullable); isT {
		return t.ColumnTypeNullable(i)
	}
	return false, false
}

func (r *guardRows) ColumnTypePrecisionScale(i int) (precision, scale int64, ok bool) {
	if t, isT := r.inner.(driver.RowsColumnTypePrecisionScale); isT {
		return t.ColumnTypePrecisionScale(i)
	}
	return 0, 0, false
}
