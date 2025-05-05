package driver

import (
	"context"
	"database/sql/driver"
)

type redshiftStatement struct {
	connection *redshiftConnection
	query      string
}

func (*redshiftStatement) Close() error {
	return nil
}

func (*redshiftStatement) NumInput() int {
	return -1
}

func (s *redshiftStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.connection.ExecContext(ctx, s.query, args)
}

func (s *redshiftStatement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.connection.QueryContext(ctx, s.query, args)
}

func (*redshiftStatement) Exec(args []driver.Value) (driver.Result, error) {
	return nil, driver.ErrSkip
}

func (*redshiftStatement) Query(args []driver.Value) (driver.Rows, error) {
	return nil, driver.ErrSkip
}
