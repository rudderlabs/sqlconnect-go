package driver

import (
	"context"
	"database/sql/driver"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/odpsclient"
)

type conn struct {
	connector *Connector
}

var (
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
)

// Ping loads the project: a real REST call that fails on a wrong endpoint,
// project, region or credential.
func (c *conn) Ping(ctx context.Context) error {
	client := c.connector.cfg.Client
	return odpsclient.SafeError(call(ctx, func() error { return client.Project(client.DefaultProjectName()).Load() }))
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
