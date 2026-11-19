package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

// Config configures a [Connector].
type Config struct {
	// Client is the configured ODPS client. Its default project is the project
	// every statement runs in.
	Client *odps.Odps
	// SubmitClient, when set, creates the jobs, with an HTTP timeout of its own:
	// a job submit is never retried, so it gets a longer timeout than the
	// polls, metadata reads and result reads made with Client. It must be for
	// the same account, endpoint and project as Client.
	SubmitClient *odps.Odps
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
	tun      *tunnel.Tunnel
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
