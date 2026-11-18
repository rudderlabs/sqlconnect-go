package driver

import (
	"context"
	"database/sql/driver"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
)

type conn struct {
	connector *Connector
}

var (
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
)

// QueryContext runs one statement, or one script that ends in a SELECT, as one
// job and streams its rows through the Instance Tunnel.
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	text, err := bind(query, args)
	if err != nil {
		return nil, err
	}
	text, err = prepareText(ctx, text)
	if err != nil {
		return nil, err
	}
	inst, err := c.connector.run(ctx, text)
	if err != nil {
		return nil, err
	}
	return c.connector.openRows(ctx, inst)
}

// CheckNamedValue passes the values the literal renderer handles through
// unchanged, and lets database/sql convert anything else (a driver.Valuer, a
// named string or integer type) with its default converter. The renderer
// refuses what remains, such as a time.Time.
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case nil, string, bool, []byte,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return nil
	}
	return driver.ErrSkip
}

// ExecContext runs one statement, or one script under [WithScript], as one
// job and waits for it to succeed.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	text, err := bind(query, args)
	if err != nil {
		return nil, err
	}
	text, err = prepareText(ctx, text)
	if err != nil {
		return nil, err
	}
	if _, err := c.connector.run(ctx, text); err != nil {
		return nil, err
	}
	return result{}, nil
}

// Ping loads the project: a real REST call that fails on a wrong endpoint,
// project, region or credential.
func (c *conn) Ping(ctx context.Context) error {
	client := c.connector.cfg.Client
	if err := call(ctx, func() error { return client.Project(client.DefaultProjectName()).Load() }); err != nil {
		return mcerr.FromSDK(err)
	}
	return nil
}

// Prepare is not supported: MaxCompute has no server-side statements.
func (c *conn) Prepare(string) (driver.Stmt, error) { return nil, sqlconnect.ErrNotSupported }

// PrepareContext is not supported: MaxCompute has no server-side statements.
func (c *conn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return nil, sqlconnect.ErrNotSupported
}

// Begin is not supported: MaxCompute has no transactions across jobs.
func (c *conn) Begin() (driver.Tx, error) { return nil, sqlconnect.ErrNotSupported }

// BeginTx is not supported: MaxCompute has no transactions across jobs.
func (c *conn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return nil, sqlconnect.ErrNotSupported
}

// Close releases nothing: connections share the connector's client.
func (c *conn) Close() error { return nil }
