package driver

import (
	"context"
	"database/sql/driver"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// ConnectorConfig contains configuration for the BigQuery connector
type ConnectorConfig struct {
	// DriverMaxRetries is passed to google-cloud-go via bigquery.WithMaxRetries().
	// Controls internal retry behavior for transient errors.
	// Default: nil (uses google-cloud-go default, which is unlimited)
	DriverMaxRetries *int

	// RetryConfig configures application-level retry for rate limit errors
	// that google-cloud-go does NOT automatically retry (e.g., invalidQuery).
	// Default: nil (no application-level retry)
	RetryConfig *RetryConfig
}

func NewConnector(projectID string, opts ...option.ClientOption) driver.Connector {
	return NewConnectorWithConfig(projectID, nil, opts...)
}

func NewConnectorWithConfig(projectID string, config *ConnectorConfig, opts ...option.ClientOption) driver.Connector {
	return &bigQueryConnector{
		projectID: projectID,
		opts:      opts,
		config:    config,
	}
}

type bigQueryConnector struct {
	projectID string
	opts      []option.ClientOption
	config    *ConnectorConfig
}

func (c *bigQueryConnector) Connect(ctx context.Context) (driver.Conn, error) {
	// Build client options, including maxRetries if configured
	opts := c.opts
	if c.config != nil && c.config.DriverMaxRetries != nil {
		// Use google-cloud-go's WithMaxRetries to limit internal retry attempts
		// This ensures google-cloud-go doesn't retry infinitely on rate limit errors
		opts = append(opts, bigquery.WithMaxRetries(*c.config.DriverMaxRetries))
	}

	client, err := bigquery.NewClient(ctx, c.projectID, opts...)
	if err != nil {
		return nil, err
	}

	var retryConfig *RetryConfig
	if c.config != nil {
		retryConfig = c.config.RetryConfig
	}

	return &bigQueryConnection{
		ctx:         ctx,
		client:      client,
		retryConfig: retryConfig,
	}, nil
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (c *bigQueryConnector) Driver() driver.Driver {
	return &bigQueryDriver{}
}
