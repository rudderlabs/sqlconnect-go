package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// Config configures a [Connector].
type Config struct {
	// Client is the configured ODPS client. Its default project is the project
	// every statement runs in.
	Client *odps.Odps
	// DefaultSchema is sent as odps.default.schema on every job when not empty.
	DefaultSchema string

	// PollInitial is the first wait between status polls; it grows by
	// PollInitial up to PollMax. Defaults: 200 ms and 1 s.
	PollInitial, PollMax time.Duration
	// TerminateWait bounds the wait for a terminated job to stop. Default 30 s.
	TerminateWait time.Duration
	// PollRetries is the number of consecutive transport failures a poll
	// survives. The poll is retried, never the job. Default 5.
	PollRetries int

	// RangeThreshold is the result size above which ReadParallelism readers
	// read consecutive ranges of one download session. Default 200,000.
	RangeThreshold int
	// ReadParallelism is the number of ranged readers. Default 4.
	ReadParallelism int
	// RangeBufferRows bounds the records each ranged reader buffers. Default 10,000.
	RangeBufferRows int
	// TunnelIdle is how long an Instance Tunnel request may wait without
	// progress: a session create or range open with no answer, or a result
	// stream that sends nothing. A stalled range is resumed. It is an idle
	// deadline, not a request timeout, so a long stream is never cut.
	// Default 60 s.
	TunnelIdle time.Duration
}

func (c *Config) applyDefaults() {
	if c.PollInitial <= 0 {
		c.PollInitial = 200 * time.Millisecond
	}
	if c.PollMax < c.PollInitial {
		c.PollMax = max(time.Second, c.PollInitial)
	}
	if c.TerminateWait <= 0 {
		c.TerminateWait = 30 * time.Second
	}
	if c.PollRetries <= 0 {
		c.PollRetries = 5
	}
	if c.RangeThreshold <= 0 {
		c.RangeThreshold = 200_000
	}
	if c.ReadParallelism <= 0 {
		c.ReadParallelism = 4
	}
	if c.RangeBufferRows <= 0 {
		c.RangeBufferRows = 10_000
	}
	if c.TunnelIdle <= 0 {
		c.TunnelIdle = 60 * time.Second
	}
}

// NewConnector returns a connector for [sql.OpenDB].
func NewConnector(cfg Config) (driver.Connector, error) {
	if cfg.Client == nil {
		return nil, errors.New("maxcompute: no ODPS client")
	}
	cfg.applyDefaults()
	return &Connector{cfg: cfg}, nil
}

// Connector opens connections that share one ODPS client. MaxCompute keeps
// no session, so a connection holds no server state.
type Connector struct {
	cfg Config

	tunnelMu sync.Mutex
	tun      *restclient.RestClient // the Instance Tunnel's; see tunnel
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
