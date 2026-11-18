package driver

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

// Config configures a [Connector].
type Config struct {
	// Client is the configured ODPS client. Its default project is the project
	// every statement runs in.
	Client *odps.Odps
	// DefaultSchema is sent as odps.default.schema on every job when not empty.
	DefaultSchema string
}

// NewConnector returns a connector for [sql.OpenDB].
func NewConnector(cfg Config) (driver.Connector, error) {
	if cfg.Client == nil {
		return nil, errors.New("maxcompute: no ODPS client")
	}
	return &Connector{cfg: cfg}, nil
}

// Connector opens connections that share one ODPS client. MaxCompute keeps
// no session, so a connection holds no server state.
type Connector struct {
	cfg Config
}

// Connect returns a new connection.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &conn{connector: c}, nil
}

// Driver returns a driver that refuses DSNs.
func (c *Connector) Driver() driver.Driver { return maxcomputeDriver{} }
