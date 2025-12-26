package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type bigQueryConnection struct {
	ctx         context.Context
	client      *bigquery.Client
	closed      bool
	bad         bool
	retryConfig *RetryConfig
}

func (connection *bigQueryConnection) GetContext() context.Context {
	return connection.ctx
}

func (connection *bigQueryConnection) Ping(ctx context.Context) error {
	datasets := connection.client.Datasets(ctx)
	if _, err := datasets.Next(); err != nil && !errors.Is(err, iterator.Done) {
		return err
	}
	return nil
}

func (connection *bigQueryConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	statement := &bigQueryStatement{connection, query}
	return statement.QueryContext(ctx, args)
}

func (connection *bigQueryConnection) Query(query string, args []driver.Value) (driver.Rows, error) {
	return nil, driver.ErrSkip
}

func (connection *bigQueryConnection) Prepare(query string) (driver.Stmt, error) {
	statement := &bigQueryStatement{connection, query}

	return statement, nil
}

func (connection *bigQueryConnection) Close() error {
	if connection.closed {
		return nil
	}
	if connection.bad {
		return driver.ErrBadConn
	}
	connection.closed = true
	return connection.client.Close()
}

func (connection *bigQueryConnection) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("bigquery: transactions are not supported")
}

func (connection *bigQueryConnection) query(query string) (*bigquery.Query, error) { // nolint: unparam
	return connection.client.Query(query), nil
}

func (connection *bigQueryConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	statement := &bigQueryStatement{connection, query}
	return statement.ExecContext(ctx, args)
}

func (bigQueryConnection) CheckNamedValue(*driver.NamedValue) error {
	return nil
}

// BigqueryClient returns the underlying bigquery.Client (for those hard to reach places...)
func (connection *bigQueryConnection) BigqueryClient() *bigquery.Client {
	return connection.client
}
