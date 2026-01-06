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
	// Build client options, including maxRetries if configured
	opts := c.opts
	if c.retryConfig != nil && c.retryConfig.MaxRetries != nil {
		// Use google-cloud-go2's WithMaxRetries to limit internal retry attempts
		// This ensures google-cloud-go doesn't retry infinitely on rate limit errors
		opts = append(opts, bigquery.WithMaxRetries(*c.retryConfig.MaxRetries))
	}

	client, err := bigquery.NewClient(ctx, c.projectID, opts...)
	if err != nil {
		return nil, err
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
