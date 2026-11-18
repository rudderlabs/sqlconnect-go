// Package driver is a database/sql driver for MaxCompute (ODPS). It wraps the
// ODPS SDK's REST client the way the BigQuery driver wraps the BigQuery client.
//
// The SDK's own sqldriver cannot be used for reverse ETL: it ignores the
// context, renders bind arguments as raw text, needs a trailing semicolon,
// fakes Ping and BeginTx, and reads rows through a path capped at 10,000 rows
// (see the MaxCompute driver LLD, section 1).
package driver

import (
	"context"
	"database/sql/driver"
	"errors"
)

// errNoDSN is returned by the [driver.Driver] that [Connector.Driver] returns:
// the driver is opened with [sql.OpenDB] and a [Connector] only.
var errNoDSN = errors.New("maxcompute: the driver is opened with a connector, not a DSN")

type maxcomputeDriver struct{}

func (maxcomputeDriver) Open(string) (driver.Conn, error) { return nil, errNoDSN }

// call runs fn, an SDK call that takes no context, and returns early with the
// context's error when ctx ends first. The SDK's HTTP timeout bounds fn, which
// then finishes in the background.
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
