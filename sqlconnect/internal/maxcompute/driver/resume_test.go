package driver

import (
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
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

func (o *fakeOpener) open(start, count int) (recordReader, error) {
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
	cfg.applyDefaults()
	r := newRows(nil, "inst1")
	r.startWith(o.open, total, cfg)
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

	t.Run("a checksum error is not resumed", func(t *testing.T) {
		o := &fakeOpener{plans: []plan{{3, errors.New("crc value is error")}}}
		_, err := readAll(t, o, n, Config{})
		require.Error(t, err)
		require.Len(t, o.opens, 1)
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
