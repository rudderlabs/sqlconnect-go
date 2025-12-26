package driver

import (
	"context"
	"database/sql/driver"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// RetryConfig contains retry configuration for BigQuery API calls
type RetryConfig struct {
	MaxRetries       *int
	MaxRetryDuration *time.Duration
}

func NewConnector(projectID string, opts ...option.ClientOption) driver.Connector {
	return NewConnectorWithRetry(projectID, nil, opts...)
}

func NewConnectorWithRetry(projectID string, retryConfig *RetryConfig, opts ...option.ClientOption) driver.Connector {
	return &bigQueryConnector{
		projectID:   projectID,
		opts:        opts,
		retryConfig: retryConfig,
	}
}

type bigQueryConnector struct {
	projectID   string
	opts        []option.ClientOption
	retryConfig *RetryConfig
}

func (c *bigQueryConnector) Connect(ctx context.Context) (driver.Conn, error) {
	client, err := bigquery.NewClient(ctx, c.projectID, c.opts...)
	if err != nil {
		return nil, err
	}

	// Apply retry configuration if provided
	if c.retryConfig != nil {
		if c.retryConfig.MaxRetries != nil {
			// Note: BigQuery client doesn't expose direct retry config via public API
			// The retry settings are controlled via gax.CallSettings which are internal
			// This configuration will be passed to the connection and used when
			// creating queries with QueryConfig
			// For now, we store it and will apply it at query execution time
		}
	}

	return &bigQueryConnection{
		ctx:         ctx,
		client:      client,
		retryConfig: c.retryConfig,
	}, nil
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (c *bigQueryConnector) Driver() driver.Driver {
	return &bigQueryDriver{}
}
