package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"regexp"
	"sync"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
)

// tunnel returns the connector's Instance Tunnel, looking its endpoint up once.
func (c *Connector) tunnel(ctx context.Context) (*tunnel.Tunnel, error) {
	c.tunnelMu.Lock()
	defer c.tunnelMu.Unlock()
	if c.tun != nil {
		return c.tun, nil
	}
	client := c.cfg.Client
	var endpoint string
	err := call(ctx, func() error {
		var err error
		endpoint, err = client.Project(client.DefaultProjectName()).GetTunnelEndpoint()
		return err
	})
	if err != nil {
		return nil, mcerr.FromSDK(err)
	}
	if err := checkTunnelEndpoint(endpoint); err != nil {
		return nil, err
	}
	c.tun = tunnel.NewTunnel(client, endpoint)
	return c.tun, nil
}

// checkTunnelEndpoint accepts only an https MaxCompute host with no port,
// user or path, such as https://dt.us-west-1.maxcompute.aliyun.com: the tunnel
// endpoint comes from the server, and every download request is signed with
// the account's key.
func checkTunnelEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	bad := mcerr.Local(mcerr.CategoryBadEndpoint, "the project returned an unexpected tunnel endpoint")
	if err != nil || u.Scheme != "https" || u.User != nil || u.Port() != "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" {
		return bad
	}
	if !tunnelHostPattern.MatchString(u.Host) {
		return bad
	}
	return nil
}

var tunnelHostPattern = regexp.MustCompile(`^[a-z0-9-]+(\.[a-z0-9-]+)*\.maxcompute\.aliyun\.com$`)

// openRows opens an Instance Tunnel download session for the job's result.
// It never sets EnableLimit, which caps a result at 10,000 rows.
func (c *Connector) openRows(ctx context.Context, inst *odps.Instance) (driver.Rows, error) {
	tun, err := c.tunnel(ctx)
	if err != nil {
		return nil, err
	}
	project := c.cfg.Client.DefaultProjectName()
	var session *tunnel.InstanceResultDownloadSession
	err = call(ctx, func() error {
		var err error
		session, err = tun.CreateInstanceResultDownloadSession(project, inst.Id(), tunnel.InstanceSessionCfg.WithDefaultDeflateCompressor())
		return err
	})
	if err != nil {
		if httpErr, ok := httpError(err); ok && httpErr.ErrorMessage != nil && httpErr.ErrorMessage.ErrorCode == "InstanceTypeNotSupported" {
			return nil, mcerr.Local(mcerr.CategoryNotAQuery, "the statement returns no rows; use Exec").WithInstance(inst.Id())
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, mcerr.Context(ctxErr, inst.Id(), "")
		}
		return nil, mcerr.FromSDK(err).WithInstance(inst.Id())
	}
	count := session.RecordCount()
	if fn := rowCountHandler(ctx); fn != nil {
		fn(int64(count))
	}
	r := newRows(session.Schema().Columns, inst.Id())
	if count > 0 {
		r.start(session, count, c.cfg)
	}
	// database/sql closes rows when ctx ends, but only after a Next that is
	// blocked in a read returns; closing the readers unblocks it.
	go func() {
		select {
		case <-ctx.Done():
			r.abort(mcerr.Context(ctx.Err(), inst.Id(), "the read stopped"))
		case <-r.done:
		}
	}()
	return r, nil
}

type item struct {
	record data.Record
	err    error
}

// rows streams a result in order. A result above RangeThreshold rows is read
// by ReadParallelism readers over consecutive ranges of one session, each
// buffering at most RangeBufferRows records; rows are emitted in range order.
type rows struct {
	columns    []tableschema.Column
	names      []string
	instanceID string

	ranges []chan item // one per range, consumed in order
	cur    int
	read   int // records returned
	want   int // records the session reported

	mu       sync.Mutex
	readers  []io.Closer
	aborted  error
	done     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newRows(columns []tableschema.Column, instanceID string) *rows {
	names := make([]string, len(columns))
	for i, c := range columns {
		names[i] = c.Name
	}
	return &rows{columns: columns, names: names, instanceID: instanceID, done: make(chan struct{})}
}

func (r *rows) start(session *tunnel.InstanceResultDownloadSession, count int, cfg Config) {
	r.want = count
	parts := 1
	if count > cfg.RangeThreshold && cfg.ReadParallelism > 1 {
		parts = cfg.ReadParallelism
	}
	size := (count + parts - 1) / parts
	for start := 0; start < count; start += size {
		n := min(size, count-start)
		ch := make(chan item, cfg.RangeBufferRows)
		r.ranges = append(r.ranges, ch)
		r.wg.Add(1)
		go r.readRange(session, start, n, ch)
	}
}

// readRange reads one range into ch and closes it.
func (r *rows) readRange(session *tunnel.InstanceResultDownloadSession, start, n int, ch chan<- item) {
	defer r.wg.Done()
	defer close(ch)
	send := func(it item) bool {
		select {
		case ch <- it:
			return true
		case <-r.done:
			return false
		}
	}
	reader, err := r.open(session, start, n)
	if err != nil {
		send(item{err: err})
		return
	}
	if reader == nil || !r.track(reader) {
		return
	}
	got := 0
	for {
		rec, err := readRecord(reader)
		if errors.Is(err, io.EOF) {
			if got != n {
				send(item{err: mcerr.Local(mcerr.CategoryData, fmt.Sprintf("the result range ended after %d of %d rows", got, n)).WithInstance(r.instanceID)})
			}
			return
		}
		if err != nil {
			send(item{err: r.readError(err)})
			return
		}
		got++
		if !send(item{record: rec}) {
			return
		}
	}
}

// open opens a range reader. The open is an HTTP request without a timeout, so
// it runs aside: when the rows stop first, open returns nil and the reader that
// may still arrive is closed.
func (r *rows) open(session *tunnel.InstanceResultDownloadSession, start, n int) (*tunnel.RecordProtocReader, error) {
	type opened struct {
		reader *tunnel.RecordProtocReader
		err    error
	}
	done := make(chan opened, 1)
	go func() {
		reader, err := session.OpenRecordReader(start, n, 0, nil)
		done <- opened{reader, err}
	}()
	select {
	case o := <-done:
		if o.err != nil {
			return nil, r.readError(o.err)
		}
		return o.reader, nil
	case <-r.done:
		go func() {
			if o := <-done; o.reader != nil {
				_ = o.reader.Close()
			}
		}()
		return nil, nil //nolint:nilnil // the rows stopped; there is nothing to read
	}
}

// readRecord reads one record and turns a panic in the SDK's decoder into an
// error: a panic must not be reachable from customer data.
func readRecord(reader *tunnel.RecordProtocReader) (rec data.Record, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = mcerr.Local(mcerr.CategoryData, "the SDK failed to decode a record")
		}
	}()
	return reader.Read()
}

func (r *rows) readError(err error) error {
	r.mu.Lock()
	aborted := r.aborted
	r.mu.Unlock()
	if aborted != nil {
		return aborted
	}
	if e, ok := errors.AsType[*mcerr.Error](err); ok {
		return e.WithInstance(r.instanceID)
	}
	return mcerr.FromSDK(err).WithInstance(r.instanceID)
}

// track registers a reader for closing; it reports false, closing the reader,
// when the rows are already stopped.
func (r *rows) track(reader io.Closer) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.done:
		_ = reader.Close()
		return false
	default:
	}
	r.readers = append(r.readers, reader)
	return true
}

// abort stops the readers with err as the error of the next Next.
func (r *rows) abort(err error) {
	r.mu.Lock()
	if r.aborted == nil {
		r.aborted = err
	}
	r.mu.Unlock()
	r.stop()
}

func (r *rows) stop() {
	r.stopOnce.Do(func() {
		close(r.done)
		r.mu.Lock()
		for _, rd := range r.readers {
			_ = rd.Close()
		}
		r.mu.Unlock()
	})
}

func (r *rows) Columns() []string { return r.names }

// Close stops the readers and waits for them.
func (r *rows) Close() error {
	r.stop()
	r.wg.Wait()
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	for r.cur < len(r.ranges) {
		it, ok := <-r.ranges[r.cur]
		if !ok {
			r.cur++
			continue
		}
		if it.err != nil {
			return it.err
		}
		for i := range dest {
			if i >= len(it.record) {
				dest[i] = nil
				continue
			}
			v, err := convertValue(it.record[i])
			if err != nil {
				if e, ok := errors.AsType[*mcerr.Error](err); ok {
					return &mcerr.Error{Category: e.Category, Message: e.Message + " (column " + r.names[i] + ")", InstanceID: r.instanceID}
				}
				return err
			}
			dest[i] = v
		}
		r.read++
		return nil
	}
	r.mu.Lock()
	aborted := r.aborted
	r.mu.Unlock()
	if aborted != nil {
		return aborted
	}
	if r.read != r.want {
		return mcerr.Local(mcerr.CategoryData, fmt.Sprintf("the result ended after %d of %d rows", r.read, r.want)).WithInstance(r.instanceID)
	}
	return io.EOF
}

// ColumnTypeDatabaseTypeName returns the MaxCompute type with its parameters,
// such as DECIMAL(22,6), ARRAY<INT> or STRUCT<`city`:STRING,`zip`:INT>.
func (r *rows) ColumnTypeDatabaseTypeName(i int) string { return r.columns[i].Type.Name() }

// ColumnTypeNullable reports that nullability is unknown.
func (r *rows) ColumnTypeNullable(int) (nullable, ok bool) { return false, false }

// ColumnTypeScanType returns the Go type of the column's values.
func (r *rows) ColumnTypeScanType(i int) reflect.Type { return scanType(r.columns[i].Type) }

// ColumnTypePrecisionScale returns a DECIMAL column's precision and scale.
func (r *rows) ColumnTypePrecisionScale(i int) (precision, scale int64, ok bool) {
	if d, isDecimal := r.columns[i].Type.(datatype.DecimalType); isDecimal {
		return int64(d.Precision), int64(d.Scale), true
	}
	return 0, 0, false
}

// ColumnTypeLength returns a VARCHAR or CHAR column's declared length.
func (r *rows) ColumnTypeLength(i int) (int64, bool) {
	switch t := r.columns[i].Type.(type) {
	case datatype.VarcharType:
		return int64(t.Length), true
	case datatype.CharType:
		return int64(t.Length), true
	}
	return 0, false
}

var (
	_ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ driver.RowsColumnTypeNullable         = (*rows)(nil)
	_ driver.RowsColumnTypeScanType         = (*rows)(nil)
	_ driver.RowsColumnTypePrecisionScale   = (*rows)(nil)
	_ driver.RowsColumnTypeLength           = (*rows)(nil)
)
