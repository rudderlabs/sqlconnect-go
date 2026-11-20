package driver

import (
	"bytes"
	"compress/flate"
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
)

// fakeStream serves records start..start+count-1 whose only value is the row
// index, and breaks after `breakAfter` records with `breakErr`.
type fakeStream struct {
	next, end  int
	breakAfter int // records to send before breaking; <0 never breaks
	breakErr   error
	sent       int
}

func (s *fakeStream) Read() (data.Record, error) {
	if s.breakAfter >= 0 && s.sent == s.breakAfter {
		return nil, s.breakErr
	}
	if s.next >= s.end {
		return nil, io.EOF
	}
	rec := data.Record{data.BigInt(s.next)}
	s.next++
	s.sent++
	return rec, nil
}
func (s *fakeStream) Close() error { return nil }

// plan says how the k-th open of a range breaks: records sent, then the error.
type plan struct {
	breakAfter int
	err        error
}

type fakeOpener struct {
	mu    sync.Mutex
	plans []plan // consumed per open; when empty the stream does not break
	opens [][2]int
	extra int // records past the range end on every stream
}

func (o *fakeOpener) open(_ context.Context, start, count int) (recordReader, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.opens = append(o.opens, [2]int{start, count})
	p := plan{breakAfter: -1}
	if len(o.plans) > 0 {
		p, o.plans = o.plans[0], o.plans[1:]
	}
	return &fakeStream{next: start, end: start + count + o.extra, breakAfter: p.breakAfter, breakErr: p.err}, nil
}

func readAll(t *testing.T, o *fakeOpener, total int, cfg Config) ([]int64, error) {
	t.Helper()
	return readAllWith(t, o.open, total, cfg)
}

func readAllWith(t *testing.T, opener rangeOpener, total int, cfg Config) ([]int64, error) {
	t.Helper()
	cfg.applyDefaults()
	r := newRows(nil, "inst1")
	r.startWith(opener, total, cfg)
	defer func() { _ = r.Close() }()
	var ids []int64
	dest := make([]driver.Value, 1)
	for {
		err := r.Next(dest)
		if errors.Is(err, io.EOF) {
			return ids, nil
		}
		if err != nil {
			return ids, err
		}
		ids = append(ids, dest[0].(int64))
	}
}

func sequence(n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = int64(i)
	}
	return out
}

func TestRangeResume(t *testing.T) {
	saved := retryBackoff
	retryBackoff = func(int) time.Duration { return time.Millisecond }
	defer func() { retryBackoff = saved }()

	reset := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
	const n = 10
	for name, tc := range map[string]struct {
		plans []plan
		opens [][2]int
	}{
		"drop before the first row":  {[]plan{{0, io.ErrUnexpectedEOF}}, [][2]int{{0, 10}, {0, 10}}},
		"drop in the middle":         {[]plan{{4, reset}}, [][2]int{{0, 10}, {4, 6}}},
		"drop before the last row":   {[]plan{{9, io.ErrUnexpectedEOF}}, [][2]int{{0, 10}, {9, 1}}},
		"clean EOF before the end":   {[]plan{{6, io.EOF}}, [][2]int{{0, 10}, {6, 4}}},
		"three drops, then complete": {[]plan{{2, reset}, {3, io.ErrUnexpectedEOF}, {1, io.EOF}}, [][2]int{{0, 10}, {2, 8}, {5, 5}, {6, 4}}},
		"drop right after reopen":    {[]plan{{5, reset}, {0, reset}}, [][2]int{{0, 10}, {5, 5}, {5, 5}}},
	} {
		t.Run(name, func(t *testing.T) {
			o := &fakeOpener{plans: tc.plans}
			ids, err := readAll(t, o, n, Config{})
			require.NoError(t, err)
			require.Equal(t, sequence(n), ids, "every row once, in order")
			require.Equal(t, tc.opens, o.opens, "each reopen starts at the first row not yet sent")
		})
	}

	t.Run("gives up after maxResumes and reports a network error", func(t *testing.T) {
		var plans []plan
		for range maxResumes + 1 {
			plans = append(plans, plan{1, reset})
		}
		o := &fakeOpener{plans: plans}
		ids, err := readAll(t, o, n, Config{})
		var e *mcerr.Error
		require.ErrorAs(t, err, &e)
		require.Equal(t, mcerr.CategoryNetwork, e.Category)
		require.Equal(t, sequence(maxResumes+1), ids, "the rows sent before giving up are the first ones, in order")
		require.Len(t, o.opens, maxResumes+1)
	})

	t.Run("a clean EOF that keeps coming is a short read", func(t *testing.T) {
		var plans []plan
		for range maxResumes + 1 {
			plans = append(plans, plan{0, io.EOF})
		}
		_, err := readAll(t, &fakeOpener{plans: plans}, n, Config{})
		require.ErrorContains(t, err, "the result range ended after 0 of 10 rows")
		var e *mcerr.Error
		require.ErrorAs(t, err, &e)
		require.Equal(t, mcerr.CategoryNetwork, e.Category, "a short read is a broken stream, not bad data")
		require.Equal(t, mcerr.CodeShortRead, e.Code)
		require.True(t, e.Retryable)
	})

	t.Run("a break after the last row, before the trailer, fails", func(t *testing.T) {
		o := &fakeOpener{plans: []plan{{n, io.ErrUnexpectedEOF}}}
		ids, err := readAll(t, o, n, Config{})
		var e *mcerr.Error
		require.ErrorAs(t, err, &e, "the trailer was never verified")
		require.Equal(t, mcerr.CategoryNetwork, e.Category)
		require.Equal(t, sequence(n), ids)
		require.Len(t, o.opens, 1, "no zero-row reopen")
	})

	t.Run("a stream with more rows than its range fails", func(t *testing.T) {
		_, err := readAll(t, &fakeOpener{extra: 1}, n, Config{})
		require.ErrorContains(t, err, "more rows than it holds")
	})

	t.Run("ranged parallel read resumes each range in order", func(t *testing.T) {
		o := &fakeOpener{plans: []plan{{100, reset}, {7, io.ErrUnexpectedEOF}, {0, reset}}}
		total := 1000
		ids, err := readAll(t, o, total, Config{RangeThreshold: 10, ReadParallelism: 4, RangeBufferRows: 5})
		require.NoError(t, err)
		require.Equal(t, sequence(total), ids)
	})
}

// racyStream records a Close that arrives while a Read is in flight, which
// the SDK's zlib reader does not survive (a data race on its state). Its
// Read takes `delay` per record, or blocks for good from record stallAt, and
// returns early with ctx's error when the range request is cancelled, as an
// HTTP body read does.
type racyStream struct {
	ctx       context.Context
	next, end int
	delay     time.Duration
	stallAt   int // <0 never stalls

	inRead           atomic.Int32
	closedDuringRead atomic.Bool
	closed           atomic.Bool
}

func (s *racyStream) Read() (data.Record, error) {
	s.inRead.Add(1)
	defer s.inRead.Add(-1)
	if s.closed.Load() {
		return nil, io.ErrUnexpectedEOF
	}
	wait := s.delay
	if s.stallAt >= 0 && s.next == s.stallAt {
		wait = time.Hour
	}
	if wait > 0 {
		select {
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		case <-time.After(wait):
		}
	}
	if s.next >= s.end {
		return nil, io.EOF
	}
	s.next++
	return data.Record{data.BigInt(s.next - 1)}, nil
}

func (s *racyStream) Close() error {
	if s.inRead.Load() > 0 {
		s.closedDuringRead.Store(true)
	}
	s.closed.Store(true)
	return nil
}

// stopDuringRead starts one range over streams made by mk, takes one row,
// waits until a Read is in flight, and closes the rows.
func stopDuringRead(t *testing.T, mk func(ctx context.Context, start, count int) *racyStream) {
	t.Helper()
	cfg := Config{RangeBufferRows: 1}
	cfg.applyDefaults()
	var mu sync.Mutex
	var streams []*racyStream
	r := newRows(nil, "inst1")
	r.startWith(func(ctx context.Context, start, count int) (recordReader, error) {
		s := mk(ctx, start, count)
		mu.Lock()
		streams = append(streams, s)
		mu.Unlock()
		return s, nil
	}, 100, cfg)
	require.NoError(t, r.Next(make([]driver.Value, 1)))
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(streams) > 0 && streams[len(streams)-1].inRead.Load() > 0
	}, 2*time.Second, time.Millisecond, "a Read is in flight")
	start := time.Now()
	require.NoError(t, r.Close())
	require.Less(t, time.Since(start), time.Second, "Close does not wait out the Read")
	mu.Lock()
	defer mu.Unlock()
	for _, s := range streams {
		require.False(t, s.closedDuringRead.Load(), "a reader was closed while its Read was in flight")
		require.True(t, s.closed.Load(), "the reading goroutine closes its reader")
	}
}

// A slow but moving stream: stop must not close the decoder under a Read.
func TestStopDoesNotCloseDuringRead(t *testing.T) {
	stopDuringRead(t, func(ctx context.Context, start, count int) *racyStream {
		return &racyStream{ctx: ctx, next: start, end: start + count, delay: 300 * time.Millisecond, stallAt: -1}
	})
}

// A stalled stream: stop cancels the range request, which ends the Read.
func TestStopEndsAStalledRead(t *testing.T) {
	stopDuringRead(t, func(ctx context.Context, start, count int) *racyStream {
		return &racyStream{ctx: ctx, next: start, end: start + count, stallAt: start + 1}
	})
}

func TestShortResultIsRetryable(t *testing.T) {
	r := newRows(nil, "inst1")
	ch := make(chan item)
	close(ch)
	r.ranges = []chan item{ch}
	r.want = 5
	err := r.Next(make([]driver.Value, 1))
	var e *mcerr.Error
	require.ErrorAs(t, err, &e)
	require.ErrorContains(t, err, "the result ended after 0 of 5 rows")
	require.Equal(t, mcerr.CategoryNetwork, e.Category)
	require.Equal(t, mcerr.CodeShortRead, e.Code)
	require.True(t, e.Retryable)
	require.Equal(t, "inst1", e.InstanceID)
}

func TestDataErrorsStayData(t *testing.T) {
	_, err := convertValue(data.VarChar{})
	var e *mcerr.Error
	require.ErrorAs(t, err, &e)
	require.Equal(t, mcerr.CategoryData, e.Category, "a VARCHAR overflow is permanent")
	require.False(t, e.Retryable)

	_, err = readAll(t, &fakeOpener{extra: 1}, 10, Config{})
	require.ErrorAs(t, err, &e)
	require.Equal(t, mcerr.CategoryData, e.Category, "more rows than the range holds stays a data error")
}

// sdkCorrupt is an SDK tunnel integrity error as the SDK returns it.
func sdkCorrupt(msg string) error { return pkgerrors.WithStack(pkgerrors.New(msg)) }

// TestCorruptStreamResume: a stream that fails an integrity check is reopened
// at the first record not yet sent. The SDK checks a record's CRC before it
// returns the record, so a damaged record is never counted or sent: the fake
// returns the error in place of the record at the break, as the SDK does.
func TestCorruptStreamResume(t *testing.T) {
	saved := retryBackoff
	retryBackoff = func(int) time.Duration { return time.Millisecond }
	defer func() { retryBackoff = saved }()

	crc := sdkCorrupt("crc value is error")
	tag := sdkCorrupt("invalid protobuf tag")
	varint := sdkCorrupt("invalid bytes for varint")
	inflate := pkgerrors.WithStack(flate.CorruptInputError(42))
	reset := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
	const n = 10
	for name, tc := range map[string]struct {
		plans []plan
		opens [][2]int
	}{
		"corrupt at the start":             {[]plan{{0, crc}}, [][2]int{{0, 10}, {0, 10}}},
		"corrupt in the middle":            {[]plan{{4, tag}}, [][2]int{{0, 10}, {4, 6}}},
		"corrupt in the last record":       {[]plan{{9, varint}}, [][2]int{{0, 10}, {9, 1}}},
		"deflate corrupt in the middle":    {[]plan{{5, inflate}}, [][2]int{{0, 10}, {5, 5}}},
		"corrupt, then dropped, then ok":   {[]plan{{2, crc}, {3, reset}, {1, inflate}}, [][2]int{{0, 10}, {2, 8}, {5, 5}, {6, 4}}},
		"corrupt again right after reopen": {[]plan{{6, crc}, {0, crc}}, [][2]int{{0, 10}, {6, 4}, {6, 4}}},
	} {
		t.Run(name, func(t *testing.T) {
			o := &fakeOpener{plans: tc.plans}
			ids, err := readAll(t, o, n, Config{})
			require.NoError(t, err)
			require.Equal(t, sequence(n), ids, "every row once, in order: no partial or duplicate row")
			require.Equal(t, tc.opens, o.opens, "each reopen starts at the first row not yet sent")
		})
	}

	requireCorrupt := func(t *testing.T, err error) {
		t.Helper()
		var e *mcerr.Error
		require.ErrorAs(t, err, &e)
		require.Equal(t, mcerr.CategoryNetwork, e.Category, "never a data error")
		require.Equal(t, mcerr.CodeStreamCorrupt, e.Code)
		require.True(t, e.Retryable)
		require.Equal(t, "inst1", e.InstanceID)
	}

	t.Run("gives up after maxResumes as MC_STREAM_CORRUPT", func(t *testing.T) {
		for name, at := range map[string]int{"start": 0, "middle": 1} {
			t.Run(name, func(t *testing.T) {
				var plans []plan
				for range maxResumes + 1 {
					plans = append(plans, plan{at, crc})
				}
				o := &fakeOpener{plans: plans}
				ids, err := readAll(t, o, n, Config{})
				requireCorrupt(t, err)
				require.Equal(t, sequence(at*(maxResumes+1)), append([]int64{}, ids...), "only whole rows, in order, before the error")
				require.Len(t, o.opens, maxResumes+1, "resumes are bounded")
			})
		}
	})

	t.Run("a corrupt trailer after the last row fails as MC_STREAM_CORRUPT", func(t *testing.T) {
		for _, msg := range []string{"checksum is invalid", "record count does not match", "invalid stream", "expect end of stream, but not"} {
			t.Run(msg, func(t *testing.T) {
				o := &fakeOpener{plans: []plan{{n, sdkCorrupt(msg)}}}
				ids, err := readAll(t, o, n, Config{})
				requireCorrupt(t, err)
				require.Equal(t, sequence(n), ids, "every row once; the error comes after them, never silently")
				require.Len(t, o.opens, 1, "no zero-row reopen")
			})
		}
	})

	t.Run("ranged parallel read resumes each corrupt range in order", func(t *testing.T) {
		o := &fakeOpener{plans: []plan{{100, crc}, {0, inflate}, {7, tag}, {3, reset}}}
		total := 1000
		ids, err := readAll(t, o, total, Config{RangeThreshold: 10, ReadParallelism: 4, RangeBufferRows: 5})
		require.NoError(t, err)
		require.Equal(t, sequence(total), ids)
	})
}

// bodyStream reads its records through the SDK's own deflate wrapper, as the
// SDK's record reader does: with a body cut inside the zlib header, the SDK
// keeps a nil zlib reader, and Read and Close panic.
type bodyStream struct {
	body io.ReadCloser
}

func cutHeaderStream() *bodyStream {
	return &bodyStream{body: tunnel.WrapByCompressor(io.NopCloser(bytes.NewReader([]byte{0x78})), "deflate")}
}

func (s *bodyStream) Read() (data.Record, error) {
	_, err := s.body.Read(make([]byte, 1))
	return nil, err
}
func (s *bodyStream) Close() error { return s.body.Close() }

// panicStream sends `before` records, then panics with p.
type panicStream struct {
	next, before int
	p            any
}

func (s *panicStream) Read() (data.Record, error) {
	if s.next == s.before {
		panic(s.p)
	}
	s.next++
	return data.Record{data.BigInt(s.next - 1)}, nil
}
func (s *panicStream) Close() error { return nil }

func TestUnreadableStreamHeader(t *testing.T) {
	saved := retryBackoff
	retryBackoff = func(int) time.Duration { return time.Millisecond }
	defer func() { retryBackoff = saved }()

	t.Run("the SDK panics on a cut header", func(t *testing.T) {
		s := cutHeaderStream()
		require.Panics(t, func() { _, _ = s.Read() }, "SDK v0.4.26 drops zlib.NewReader's error; if this stops panicking, the guard may be removable")
		require.Panics(t, func() { _ = s.Close() })
	})

	const n = 10
	t.Run("a cut header is resumed, and its panicking Close is contained", func(t *testing.T) {
		var mu sync.Mutex
		var opens [][2]int
		opener := func(_ context.Context, start, count int) (recordReader, error) {
			mu.Lock()
			defer mu.Unlock()
			opens = append(opens, [2]int{start, count})
			if len(opens) == 1 {
				return cutHeaderStream(), nil
			}
			return &fakeStream{next: start, end: start + count, breakAfter: -1}, nil
		}
		ids, err := readAllWith(t, opener, n, Config{})
		require.NoError(t, err)
		require.Equal(t, sequence(n), ids)
		require.Equal(t, [][2]int{{0, 10}, {0, 10}}, opens)
	})

	t.Run("a cut header every time fails as MC_STREAM_CORRUPT", func(t *testing.T) {
		opens := 0
		opener := func(context.Context, int, int) (recordReader, error) { opens++; return cutHeaderStream(), nil }
		_, err := readAllWith(t, opener, n, Config{})
		var e *mcerr.Error
		require.ErrorAs(t, err, &e)
		require.Equal(t, mcerr.CategoryNetwork, e.Category)
		require.Equal(t, mcerr.CodeStreamCorrupt, e.Code)
		require.True(t, e.Retryable)
		require.Equal(t, maxResumes+1, opens)
	})

	var nilDeref any
	func() {
		defer func() { nilDeref = recover() }()
		var p *int
		_ = *p //nolint:govet // a real nil dereference, for its runtime.Error
	}()
	require.NotNil(t, nilDeref)

	for name, tc := range map[string]struct {
		before int
		p      any
	}{
		"a nil dereference after a record is a decode error": {3, nilDeref},
		"another panic on the first read is a decode error":  {0, "index out of range"},
	} {
		t.Run(name, func(t *testing.T) {
			opens := 0
			opener := func(context.Context, int, int) (recordReader, error) {
				opens++
				return &panicStream{before: tc.before, p: tc.p}, nil
			}
			ids, err := readAllWith(t, opener, n, Config{})
			var e *mcerr.Error
			require.ErrorAs(t, err, &e)
			require.Equal(t, mcerr.CategoryData, e.Category)
			require.False(t, e.Retryable)
			require.Equal(t, 1, opens, "not resumed")
			require.Equal(t, sequence(tc.before), append([]int64{}, ids...))
		})
	}
}
