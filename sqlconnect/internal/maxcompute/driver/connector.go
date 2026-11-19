package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
)

// Config configures a [Connector].
type Config struct {
	// Client is the configured ODPS client. Its default project is the project
	// every statement runs in.
	Client *odps.Odps
	// NewSubmitClient, when set, makes the client that creates the jobs, for
	// the same account, endpoint and project as Client. A submit is a POST
	// and is never retried, so it must not reuse a pooled connection that the
	// load balancer has silently dropped (RC3 R1 hung 30 s on one): the
	// connector makes a new submit client, with a new HTTP transport, when the
	// last one has been idle longer than SubmitIdle. Polls, results,
	// terminates and tunnel reads stay on Client, whose calls retry.
	NewSubmitClient func() (*odps.Odps, error)
	// SubmitIdle is the idle time after which the submit client is replaced.
	// Default 20 s.
	SubmitIdle time.Duration
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
	if c.SubmitIdle <= 0 {
		c.SubmitIdle = 20 * time.Second
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
	return &Connector{cfg: cfg, now: time.Now}, nil
}

// Connector opens connections that share one ODPS client. MaxCompute keeps
// no session, so a connection holds no server state.
type Connector struct {
	cfg Config

	tunnelMu sync.Mutex
	tun      *tunnel.Tunnel

	submitMu     sync.Mutex
	submitClient *odps.Odps
	submitUsed   time.Time
	now          func() time.Time // time.Now; tests replace it
}

// submitter returns the client to create a job with: the main client when
// no NewSubmitClient is set, else the current submit client, made anew when
// it has been idle longer than SubmitIdle. The replaced client's idle
// connections are not reachable through the SDK; they close when the server
// or the TCP keep-alive drops them.
func (c *Connector) submitter() (*odps.Odps, error) {
	if c.cfg.NewSubmitClient == nil {
		return c.cfg.Client, nil
	}
	c.submitMu.Lock()
	defer c.submitMu.Unlock()
	now := c.now()
	if c.submitClient == nil || now.Sub(c.submitUsed) > c.cfg.SubmitIdle {
		client, err := c.cfg.NewSubmitClient()
		if err == nil && client == nil {
			err = errors.New("NewSubmitClient returned no client")
		}
		if err != nil {
			return nil, mcerr.LocalWrap(mcerr.CategoryInternal, "the submit client cannot be made", err)
		}
		c.submitClient = client
	}
	c.submitUsed = now
	return c.submitClient, nil
}

// submitDone marks the submit client as used when a submit returns, so the
// idle time counts from the end of the last request.
func (c *Connector) submitDone(client *odps.Odps) {
	c.submitMu.Lock()
	defer c.submitMu.Unlock()
	if client == c.submitClient {
		c.submitUsed = c.now()
	}
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
