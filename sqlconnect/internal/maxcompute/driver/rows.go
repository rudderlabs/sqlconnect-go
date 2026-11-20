package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
)

// tunnel returns the rest client of the connector's Instance Tunnel, looking
// its endpoint up once. Its http.Client has an idle deadline (tunnelhttp.go);
// every copy of the returned value shares it.
func (c *Connector) tunnel(ctx context.Context) (restclient.RestClient, error) {
	c.tunnelMu.Lock()
	defer c.tunnelMu.Unlock()
	if c.tun != nil {
		return *c.tun, nil
	}
	client := c.cfg.Client
	var endpoint string
	err := retryCall(ctx, func() error {
		var err error
		endpoint, err = client.Project(client.DefaultProjectName()).GetTunnelEndpoint()
		return err
	})
	if err != nil {
		return restclient.RestClient{}, mcerr.FromSDK(err)
	}
	if err := checkTunnelEndpoint(endpoint); err != nil {
		return restclient.RestClient{}, err
	}
	if err := c.setTunnel(endpoint); err != nil {
		return restclient.RestClient{}, err
	}
	return *c.tun, nil
}

// setTunnel builds the tunnel's rest client for endpoint, as the SDK's
// tunnel.NewTunnel would, but with the idle-deadline http.Client.
func (c *Connector) setTunnel(endpoint string) error {
	rc := restclient.NewOdpsRestClient(c.cfg.Client.Account(), endpoint)
	rc.HttpTimeout = 0 // unused: the http.Client below is ours
	hc := idleHTTPClient(c.cfg.TunnelIdle, tunnel.DefaultTcpConnectionTimeout)
	if err := setHTTPClient(&rc, hc); err != nil {
		return mcerr.Local(mcerr.CategoryInternal, err.Error())
	}
	c.tun, c.tunHTTP = &rc, hc
	return nil
}

// The check deliberately refuses private and VPC tunnel endpoints in v1, as
// the LLD says: the account endpoint is the public one.
//
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
	rc, err := c.tunnel(ctx)
	if err != nil {
		return nil, err
	}
	project := c.cfg.Client.DefaultProjectName()
	var session *tunnel.InstanceResultDownloadSession
	// A second download session for the same instance is harmless, so the
	// create is retried after a transport error, including the idle timeout
	// of a create the server never answers.
	err = retryCall(ctx, func() error {
		var err error
		session, err = tunnel.CreateInstanceResultDownloadSession(project, inst.Id(), "", rc, tunnel.InstanceSessionCfg.WithDefaultDeflateCompressor())
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
		c.tunnelMu.Lock()
		hc := c.tunHTTP
		c.tunnelMu.Unlock()
		r.startWith(sessionOpener(session, hc), count, c.cfg)
	}
	// database/sql closes rows when ctx ends, but only after a Next that is
	// blocked in a read returns; stopping the rows cancels the range
	// requests, which unblocks it.
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

	opener  rangeOpener
	backoff func(int) time.Duration // wait before resume or reopen n
	ranges  []chan item             // one per range, consumed in order
	cur     int
	read    int // records returned
	want    int // records the session reported

	mu      sync.Mutex
	aborted error
	// done closes and ctx is cancelled when the rows stop. ctx is the
	// context of every range request, so a stop aborts a Read in progress
	// at the HTTP layer instead of closing the decoder under it.
	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newRows(columns []tableschema.Column, instanceID string) *rows {
	names := make([]string, len(columns))
	for i, c := range columns {
		names[i] = c.Name
	}
	// The timings are read once here, not by the reader goroutines, which can
	// outlive the call that started them.
	ctx, cancel := context.WithCancel(context.Background())
	return &rows{columns: columns, names: names, instanceID: instanceID, done: make(chan struct{}), ctx: ctx, cancel: cancel, backoff: retryBackoff}
}

// recordReader reads the records of one range; the SDK's *tunnel.RecordProtocReader is one.
type recordReader interface {
	Read() (data.Record, error)
	Close() error
}

// rangeOpener opens a reader over count records from start. The request,
// and every Read of the reader, ends when ctx is cancelled.
type rangeOpener func(ctx context.Context, start, count int) (recordReader, error)

// sessionOpener opens ranges of session through hc, the tunnel's client.
// The SDK builds the range request without a context, so each open uses a
// copy of the session whose client puts ctx on every request; the copy
// shares hc's transport and its connections.
func sessionOpener(session *tunnel.InstanceResultDownloadSession, hc *http.Client) rangeOpener {
	return func(ctx context.Context, start, count int) (recordReader, error) {
		s := *session
		if err := setHTTPClient(&s.RestClient, &http.Client{Transport: contextTransport{base: hc.Transport, ctx: ctx}}); err != nil {
			return nil, mcerr.Local(mcerr.CategoryInternal, err.Error())
		}
		reader, err := s.OpenRecordReader(start, count, 0, nil)
		if err != nil {
			return nil, err
		}
		return reader, nil
	}
}

// contextTransport sends every request with ctx.
type contextTransport struct {
	base http.RoundTripper
	ctx  context.Context
}

func (t contextTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.base.RoundTrip(req.WithContext(t.ctx))
}

func (r *rows) startWith(opener rangeOpener, count int, cfg Config) {
	r.opener = opener
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
		go r.readRange(start, n, ch)
	}
}

// maxResumes bounds how often one range is reopened after its stream broke.
const maxResumes = 3

// readRange reads one range of n records from start into ch, in order, and
// closes ch. When the stream breaks mid-range (a transport error, an
// unexpected EOF, a failed integrity check, or a clean end before n records),
// the range is reopened at start+got for the n-got records still missing, up
// to maxResumes times. A
// record is counted and sent only once the SDK has decoded it whole, so a
// reopened stream starts exactly at the first record not yet sent: no record
// is sent twice or skipped, and the range still ends only when got == n.
func (r *rows) readRange(start, n int, ch chan<- item) {
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
	got, resumes := 0, 0
	for {
		opened, err := r.open(start+got, n-got)
		if err != nil {
			send(item{err: err})
			return
		}
		if opened == nil {
			return
		}
		// Only this goroutine closes its reader, and only after its Read
		// returned: the SDK's decoder (a zlib stream over the HTTP body) must
		// not be closed during a Read. A stop cancels the range request,
		// which ends a Read in progress.
		reader := opened
		var readErr error
		stopped := false
		for {
			var rec data.Record
			rec, readErr = reader.Read()
			if readErr != nil {
				break
			}
			if got == n {
				readErr = errMoreRows()
				break
			}
			got++
			if !send(item{record: rec}) {
				stopped = true
				break
			}
		}
		_ = reader.Close()
		if stopped {
			return
		}
		// A clean end after the range's last record completes it. The SDK
		// checks the stream's row count and checksum only when it reads the
		// trailer, and a body that ends cleanly without one also gives EOF,
		// so such a body passes here: every record still passed its own
		// CRC and got == n, but the stream-level check is skipped. The SDK
		// v0.4.26 reader does not expose whether it reached the trailer.
		if errors.Is(readErr, io.EOF) && got == n {
			return
		}
		// A stream that broke after its last record but before or inside its
		// trailer (the record count and checksum the SDK verifies) is not
		// resumed: there is nothing left to reopen. It fails as a network
		// error (MC_STREAM_CORRUPT for a failed check), so the caller rereads.
		if got < n && resumable(readErr) && resumes < maxResumes {
			resumes++
			select {
			case <-r.done:
				return
			case <-time.After(r.backoff(resumes)):
				continue
			}
		}
		if errors.Is(readErr, io.EOF) {
			send(item{err: mcerr.ShortRead(fmt.Sprintf("the result range ended after %d of %d rows", got, n)).WithInstance(r.instanceID)})
			return
		}
		send(item{err: r.readError(readErr)})
		return
	}
}

// errMoreRows is a stream that sent more records than its range asked for. A
// fresh error each time: readError sets the instance id on it.
func errMoreRows() error {
	return mcerr.Local(mcerr.CategoryData, "the result range sent more rows than it holds")
}

// resumable reports a broken stream that a reopened range can continue: a
// transport error, an unexpected EOF, an EOF before the range's last record,
// or a failed integrity check (a record CRC, the count or the checksum, or a
// deflate stream that does not decode). The SDK checks a record's CRC before
// it returns the record, so a damaged record is never counted or sent and the
// reopened range starts at it. A decoder panic and a range with too many
// records are driver errors (*mcerr.Error, CategoryData) and are not resumed.
func resumable(err error) bool {
	if _, ok := errors.AsType[*mcerr.Error](err); ok {
		return false
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || mcerr.IsTransport(err) || mcerr.IsStreamCorrupt(err)
}

// open opens a range reader. The open is an HTTP request without a timeout, so
// it runs aside: when the rows stop first, open returns nil and the reader that
// may still arrive is closed.
func (r *rows) open(start, n int) (recordReader, error) {
	type opened struct {
		reader recordReader
		err    error
	}
	done := make(chan opened, 1)
	go func() {
		var reader recordReader
		var err error
		for attempt := 0; ; attempt++ {
			reader, err = r.opener(r.ctx, start, n)
			if err == nil {
				reader = &guardedReader{recordReader: reader}
				break
			}
			if attempt >= transportRetries || !mcerr.IsTransport(err) {
				break
			}
			select {
			case <-r.done:
			case <-time.After(r.backoff(attempt + 1)):
				continue
			}
			break
		}
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

// guardedReader keeps a panic in the SDK's reader from escaping: a panic must
// not be reachable from customer data or from a damaged stream.
//
// SDK v0.4.26 builds its deflate reader with zlib.NewReader and drops the
// error, so a body that is cut or damaged inside its two-byte zlib header
// leaves a nil reader: the first Read and every Close then panic with a nil
// dereference. Such a panic on a reader that has not yet returned a record is
// a damaged stream (mcerr.ErrUnreadableStream, resumed like any corruption);
// any other panic is the SDK failing to decode a value, a data error. The
// panicking Close leaks that one HTTP body: the SDK closes it only after the
// nil reader.
type guardedReader struct {
	recordReader
	read bool // a record was read; touched only by the reading goroutine
}

func (g *guardedReader) Read() (rec data.Record, err error) {
	defer func() {
		if p := recover(); p != nil {
			if !g.read && isNilDeref(p) {
				err = mcerr.ErrUnreadableStream
				return
			}
			err = mcerr.Local(mcerr.CategoryData, "the SDK failed to decode a record")
		}
	}()
	rec, err = g.recordReader.Read()
	if err == nil {
		g.read = true
	}
	return rec, err
}

func (g *guardedReader) Close() (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = mcerr.Local(mcerr.CategoryNetwork, "the SDK failed to close a result stream")
		}
	}()
	return g.recordReader.Close()
}

func isNilDeref(p any) bool {
	e, ok := p.(runtime.Error)
	return ok && strings.Contains(e.Error(), "nil pointer dereference")
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

// abort stops the readers with err as the error of the next Next.
func (r *rows) abort(err error) {
	r.mu.Lock()
	if r.aborted == nil {
		r.aborted = err
	}
	r.mu.Unlock()
	r.stop()
}

// stop ends the read: the range goroutines see done between records, and
// cancelling ctx aborts their range requests, so a Read in progress returns
// with an error. Each goroutine then closes its own reader.
func (r *rows) stop() {
	r.stopOnce.Do(func() {
		close(r.done)
		r.cancel()
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
				return mcerr.Local(mcerr.CategoryData, fmt.Sprintf("a record holds %d values for %d columns", len(it.record), len(dest))).WithInstance(r.instanceID)
			}
			v, err := convertValue(it.record[i])
			if err != nil {
				if e, ok := errors.AsType[*mcerr.Error](err); ok {
					return mcerr.Local(e.Category, e.Message+" (column "+r.names[i]+")").WithInstance(r.instanceID)
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
		return mcerr.ShortRead(fmt.Sprintf("the result ended after %d of %d rows", r.read, r.want)).WithInstance(r.instanceID)
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
