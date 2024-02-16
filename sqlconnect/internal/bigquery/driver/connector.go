package driver

import (
	"context"
	"database/sql/driver"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

func NewConnector(projectID string, opts ...option.ClientOption) driver.Connector {
	return &bigQueryConnector{
		projectID: projectID,
		opts:      opts,
	}
}

type bigQueryConnector struct {
	projectID string
	opts      []option.ClientOption
}

func (c *bigQueryConnector) Connect(ctx context.Context) (driver.Conn, error) {
	client, err := bigquery.NewClient(ctx, c.projectID, c.opts...)
	if err != nil {
		return nil, err
	}

	return &bigQueryConnection{
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
