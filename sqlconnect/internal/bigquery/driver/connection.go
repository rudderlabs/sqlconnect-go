package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/cenkalti/backoff/v4"
	"google.golang.org/api/iterator"
)

type bigQueryConnection struct {
	config Config
	ctx    context.Context
	client *bigquery.Client
	closed bool
	bad    bool
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

// readWithBackoff will retry the read operation if the error is [jobRateLimitExceeded] and the config is set to retry rate limit errors
//
// TODO: this should no longer be needed once this fix is released:
// https://github.com/googleapis/google-cloud-go/pull/9726
func (connection *bigQueryConnection) readWithBackoff(ctx context.Context, query *bigquery.Query) (it *bigquery.RowIterator, err error) {
	if !connection.config.JobRateLimitExceededRetryEnabled {
		return query.Read(ctx)
	}
	// mimicking google's own retry backoff settings
	// https://github.com/googleapis/google-cloud-go/blob/b2e704d9d287445304d2b6030b6e35a4eb8be80a/bigquery/bigquery.go#L236
	retry := backoff.WithContext(backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(1*time.Second),
		backoff.WithMaxInterval(32*time.Second),
		backoff.WithMultiplier(2),
	), ctx)
	_ = backoff.Retry(func() error {
		it, err = query.Read(ctx)
		if err != nil && (strings.Contains(err.Error(), "jobRateLimitExceeded")) {
			return err
		}
		return nil
	}, retry)
	return it, err
}
