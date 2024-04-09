package driver

import (
	"context"
	"database/sql/driver"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type Config struct {
	JobRateLimitExceededRetryEnabled bool // Enable jobRateLimitExceeded retries: default false
}

func NewConnector(projectID string, config Config, opts ...option.ClientOption) driver.Connector {
	return &bigQueryConnector{
		projectID: projectID,
		config:    config,
		opts:      opts,
	}
}

type bigQueryConnector struct {
	projectID string
	config    Config
	opts      []option.ClientOption
}

func (c *bigQueryConnector) Connect(ctx context.Context) (driver.Conn, error) {
	client, err := bigquery.NewClient(ctx, c.projectID, c.opts...)
	if err != nil {
		return nil, err
	}

	return &bigQueryConnection{
		config: c.config,
		ctx:    ctx,
		client: client,
	}, nil
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (c *bigQueryConnector) Driver() driver.Driver {
	return &bigQueryDriver{}
}
