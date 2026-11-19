package driver

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type logLine struct {
	level, msg string
	fields     map[string]any
}

type recordingLogger struct {
	mu    sync.Mutex
	lines []logLine
}

func (r *recordingLogger) add(level, msg string, kv []any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := map[string]any{}
	for i := 0; i+1 < len(kv); i += 2 {
		f[kv[i].(string)] = kv[i+1]
	}
	r.lines = append(r.lines, logLine{level, msg, f})
}
func (r *recordingLogger) Infow(msg string, kv ...any) { r.add("info", msg, kv) }
func (r *recordingLogger) Warnw(msg string, kv ...any) { r.add("warn", msg, kv) }

// noSecrets asserts that no field value holds the SQL or a literal from it.
func noSecrets(t *testing.T, l logLine, forbidden ...string) {
	t.Helper()
	for k, v := range l.fields {
		for _, f := range forbidden {
			require.NotContains(t, fmt.Sprint(v), f, "field %s", k)
		}
	}
}

func TestJobLogs(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		f := newFakeODPS(t)
		rec := &recordingLogger{}
		_, err := f.db(t).ExecContext(WithLogger(context.Background(), rec), "INSERT INTO t VALUES ('SENTINEL_VALUE')")
		require.NoError(t, err)
		require.Len(t, rec.lines, 2)
		sub, fin := rec.lines[0], rec.lines[1]
		require.Equal(t, EventJobSubmitted, sub.msg)
		require.Equal(t, "info", sub.level)
		require.Equal(t, "inst1", sub.fields["mc_instance_id"])
		require.Equal(t, "exec", sub.fields["kind"])
		require.Contains(t, sub.fields, "submit_ms")
		require.Equal(t, EventJobFinished, fin.msg)
		require.Equal(t, StatusSuccess, fin.fields["status"])
		require.Contains(t, fin.fields, "wall_ms")
		for _, l := range rec.lines {
			noSecrets(t, l, "SENTINEL_VALUE", "INSERT")
		}
	})

	t.Run("script failure", func(t *testing.T) {
		f := newFakeODPS(t)
		f.set(func(f *fakeODPS) {
			f.statuses = []string{"Failed"}
			f.failureText = "ODPS-0130131:[1,15] Table not found - table p.s.secret_table cannot be resolved"
		})
		rec := &recordingLogger{}
		ctx := WithScript(WithLogger(context.Background(), rec))
		_, err := f.db(t).ExecContext(ctx, "CREATE TABLE a (x BIGINT); INSERT INTO a SELECT 'SENTINEL_VALUE';")
		require.Error(t, err)
		require.Len(t, rec.lines, 2)
		require.Equal(t, "script", rec.lines[0].fields["kind"])
		require.EqualValues(t, 2, rec.lines[0].fields["statement_count"])
		fin := rec.lines[1]
		require.Equal(t, "warn", fin.level)
		require.Equal(t, StatusFailed, fin.fields["status"])
		require.Equal(t, "not_found", fin.fields["category"])
		require.Equal(t, "ODPS-0130131", fin.fields["code"])
		for _, l := range rec.lines {
			noSecrets(t, l, "SENTINEL_VALUE", "secret_table", "CREATE")
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		f := newFakeODPS(t)
		f.set(func(f *fakeODPS) { f.statuses = []string{"Running"} })
		rec := &recordingLogger{}
		ctx, cancel := context.WithTimeout(WithLogger(context.Background(), rec), 50*time.Millisecond)
		defer cancel()
		rows, err := f.db(t).QueryContext(ctx, "SELECT 1")
		if err == nil {
			_ = rows.Close()
			require.NoError(t, rows.Err())
		}
		require.Error(t, err)
		fin := rec.lines[len(rec.lines)-1]
		require.Equal(t, StatusCancelled, fin.fields["status"])
		require.Equal(t, "query", fin.fields["kind"])
		require.Equal(t, "landed", fin.fields["stop_outcome"])
	})

	t.Run("submit rejected", func(t *testing.T) {
		f := newFakeODPS(t)
		f.set(func(f *fakeODPS) {
			f.submitStatus = 400
			f.submitBody = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>ParseError</Code><Message><![CDATA[ODPS-0130161:[1,1] Parse exception - invalid token 'selec']]></Message></Error>`
		})
		rec := &recordingLogger{}
		_, err := f.db(t).ExecContext(WithLogger(context.Background(), rec), "selec 1")
		require.Error(t, err)
		require.Len(t, rec.lines, 1, "no submitted event without a job")
		require.Equal(t, StatusSubmitFailed, rec.lines[0].fields["status"])
		require.Equal(t, "syntax", rec.lines[0].fields["category"])
	})

	t.Run("no logger, no events", func(t *testing.T) {
		f := newFakeODPS(t)
		_, err := f.db(t).ExecContext(context.Background(), "SELECT 1")
		require.NoError(t, err, "jobs run unchanged without a logger")
	})
}
