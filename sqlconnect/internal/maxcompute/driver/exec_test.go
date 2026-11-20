package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
)

func TestExecSubmitsWithHints(t *testing.T) {
	f := newFakeODPS(t)
	f.set(func(f *fakeODPS) { f.statuses = []string{"Running", "Running", "Success"} })
	db := f.db(t)

	var ids []string
	ctx := WithInstanceIDHandler(context.Background(), func(id string) { ids = append(ids, id) })
	_, err := db.ExecContext(ctx, "CREATE TABLE t AS SELECT 1 AS a -- note")
	require.NoError(t, err)
	require.Equal(t, []string{"inst1"}, ids, "the instance id is handed over before the wait")

	s := f.snapshot()
	require.Len(t, s.submits, 1)
	require.Equal(t, "CREATE TABLE t AS SELECT 1 AS a -- note\n;", s.submits[0].Query, "the terminator is added after a line comment")
	require.Equal(t, map[string]string{
		"odps.sql.type.system.odps2":      "true",
		"odps.sql.decimal.odps2":          "true",
		"odps.namespace.schema":           "true",
		"odps.sql.validate.orderby.limit": "false",
		"odps.default.schema":             "s1",
		"rudderstack.job.tag":             "rudderstack-retl",
	}, s.submits[0].Hints, "the fixed map, the default schema and the job tag; never a time zone")
	require.Equal(t, 3, s.polls)
}

func TestExecPerCallHints(t *testing.T) {
	f := newFakeODPS(t)
	db := f.db(t)
	ctx, err := WithHints(context.Background(), map[string]string{"odps.sql.allow.fullscan": "true"})
	require.NoError(t, err)
	_, err = db.ExecContext(WithScript(ctx), "CREATE TABLE a AS SELECT 1; INSERT INTO a SELECT 2;")
	require.NoError(t, err)
	h := f.snapshot().submits[0].Hints
	require.NotContains(t, h, "odps.sql.timezone")
	require.Equal(t, "true", h["odps.sql.allow.fullscan"])
	require.Equal(t, "script", h["odps.sql.submit.mode"])
	require.Equal(t, "CREATE TABLE a AS SELECT 1; INSERT INTO a SELECT 2;", f.snapshot().submits[0].Query, "a script is sent unchanged")
}

func TestWithHintsRefuses(t *testing.T) {
	ctx := context.Background()
	for k, v := range map[string]string{
		"odps.sql.type.system.odps2":      "false",
		"odps.sql.validate.orderby.limit": "true",
		"odps.namespace.schema":           "false",
		"odps.default.schema":             "other",
		"odps.sql.submit.mode":            "script",
		"odps.sql.hive.compatible":        "true",
		"odps.sql.timezone":               "UTC",
	} {
		_, err := WithHints(ctx, map[string]string{k: v})
		require.ErrorIs(t, err, ErrHintNotAllowed, k)
	}
	_, err := WithHints(ctx, map[string]string{"odps.sql.allow.fullscan": "yes"})
	require.ErrorIs(t, err, ErrHintValue)
	outer, err := WithHints(ctx, map[string]string{"odps.sql.allow.fullscan": "true"})
	require.NoError(t, err)
	inner, err := WithHints(outer, map[string]string{"odps.sql.allow.fullscan": "false"})
	require.NoError(t, err)
	require.Equal(t, "false", jobHints(inner, "")["odps.sql.allow.fullscan"], "an inner WithHints replaces the outer value")
	require.Equal(t, "true", jobHints(outer, "")["odps.sql.allow.fullscan"], "the outer context is unchanged")
}

func TestExecRefusesSeveralStatementsWithoutScript(t *testing.T) {
	f := newFakeODPS(t)
	db := f.db(t)
	_, err := db.ExecContext(context.Background(), "SET odps.sql.allow.fullscan=true; SELECT 1")
	require.ErrorIs(t, err, ErrMultipleStatements)
	require.Empty(t, f.snapshot().submits, "no job is created")

	_, err = db.ExecContext(context.Background(), "SELECT ';' AS a; -- a trailing separator and comment are one statement")
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), "  -- only a comment")
	require.Error(t, err)
}

func TestExecJobFailure(t *testing.T) {
	f := newFakeODPS(t)
	f.set(func(f *fakeODPS) {
		f.statuses = []string{"Running", "Failed"}
		f.failureText = "ODPS-0130131:[1,15] Table not found - table p.s.t cannot be resolved\nDebugingInfo: abcd"
	})
	_, err := f.db(t).ExecContext(context.Background(), "SELECT * FROM s.t")
	var e *mcerr.Error
	require.ErrorAs(t, err, &e)
	require.Equal(t, "inst1", e.InstanceID)
	require.Contains(t, err.Error(), "ODPS-0130131")
	require.NotContains(t, err.Error(), "DebugingInfo")
}

func TestExecSynchronousFailure(t *testing.T) {
	f := newFakeODPS(t)
	f.set(func(f *fakeODPS) { f.syncResult = "ODPS-0130161:[1,1] Parse exception - invalid token 'selec'" })
	_, err := f.db(t).ExecContext(context.Background(), "selec 1")
	require.ErrorContains(t, err, "ODPS-0130161")
	require.Zero(t, f.snapshot().polls)
}

func TestExecSubmitRejected(t *testing.T) {
	f := newFakeODPS(t)
	f.set(func(f *fakeODPS) {
		f.submitStatus = 400
		f.submitBody = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>ParseError</Code><Message><![CDATA[ODPS-0130161:[1,1] Parse exception - invalid token 'selec']]></Message></Error>`
	})
	_, err := f.db(t).ExecContext(context.Background(), "selec 1")
	var e *mcerr.Error
	require.ErrorAs(t, err, &e)
	require.False(t, e.OutcomeUnknown, "a rejected submit created no job")
	require.Contains(t, err.Error(), "ODPS-0130161")
}

func TestExecPollRetriesTransportFailures(t *testing.T) {
	f := newFakeODPS(t)
	f.set(func(f *fakeODPS) { f.pollFailures = 2; f.statuses = []string{"Success"} })
	_, err := f.db(t).ExecContext(context.Background(), "SELECT 1")
	require.NoError(t, err)
	s := f.snapshot()
	require.Len(t, s.submits, 1, "the poll is retried, never the job")
	require.Equal(t, 3, s.polls)

	f2 := newFakeODPS(t)
	f2.set(func(f *fakeODPS) { f.pollFailures = 10 })
	_, err = f2.db(t, func(c *Config) { c.PollRetries = 2 }).ExecContext(context.Background(), "SELECT 1")
	var e *mcerr.Error
	require.ErrorAs(t, err, &e)
	require.Equal(t, mcerr.CategoryNetwork, e.Category)
	require.Len(t, f2.snapshot().submits, 1)
}

func TestExecPollGiveUpStopsTheJob(t *testing.T) {
	t.Run("transport failures past PollRetries", func(t *testing.T) {
		f := newFakeODPS(t)
		f.set(func(f *fakeODPS) { f.pollFailures = 10; f.statuses = []string{"Running"} })
		_, err := f.db(t, func(c *Config) { c.PollRetries = 2 }).ExecContext(context.Background(), "SELECT 1")
		var e *mcerr.Error
		require.ErrorAs(t, err, &e)
		require.Equal(t, mcerr.CategoryNetwork, e.Category, "the poll error is returned, not the stop's")
		require.Equal(t, "inst1", e.InstanceID)
		require.NotEmpty(t, e.Stop, "the stop attempt is reported")
		require.Eventually(t, func() bool { return f.snapshot().terminates == 1 }, 3*time.Second, 10*time.Millisecond)
	})

	t.Run("an HTTP 500 from the status poll", func(t *testing.T) {
		f := newFakeODPS(t)
		f.set(func(f *fakeODPS) { f.statuses = []string{"Running"}; f.pollStatus = http.StatusInternalServerError })
		start := time.Now()
		_, err := f.db(t).ExecContext(context.Background(), "SELECT 1")
		var e *mcerr.Error
		require.ErrorAs(t, err, &e)
		require.Equal(t, "inst1", e.InstanceID)
		require.NotEqual(t, mcerr.CategoryCancelled, e.Category, "the poll error is returned")
		require.Less(t, time.Since(start), 2*time.Second, "the stop is bounded by TerminateWait")
		require.Eventually(t, func() bool { return f.snapshot().terminates == 1 }, 3*time.Second, 10*time.Millisecond)
	})
}

func TestExecCancelTerminates(t *testing.T) {
	f := newFakeODPS(t)
	f.set(func(f *fakeODPS) { f.statuses = []string{"Running"} })
	db := f.db(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := db.ExecContext(ctx, "SELECT 1")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second+100*time.Millisecond, "returns within deadline + TerminateWait")
	var e *mcerr.Error
	require.ErrorAs(t, err, &e)
	require.Equal(t, mcerr.CategoryCancelled, e.Category)
	require.Equal(t, "inst1", e.InstanceID)
	require.Contains(t, err.Error(), "terminated")
	s := f.snapshot()
	require.Equal(t, 1, s.terminates)
	require.True(t, s.terminated)
}

func TestExecCancelledBeforeSubmit(t *testing.T) {
	f := newFakeODPS(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := f.db(t).ExecContext(ctx, "SELECT 1")
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, f.snapshot().submits)
}

func TestStopInstance(t *testing.T) {
	f := newFakeODPS(t)
	f.set(func(f *fakeODPS) { f.statuses = []string{"Running"} })
	outcome, err := StopInstance(context.Background(), f.client(), "inst1", time.Second)
	require.NoError(t, err)
	require.Equal(t, sqlconnect.StopLanded, outcome)

	f.set(func(f *fakeODPS) { f.terminateStatus = 409 })
	outcome, err = StopInstance(context.Background(), f.client(), "inst1", time.Second)
	require.NoError(t, err, "a second terminate is a no-op")
	require.Equal(t, sqlconnect.StopMissed, outcome)

	f.set(func(f *fakeODPS) {
		f.terminateBody = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>ConcurrentModification</Code><Message><![CDATA[busy]]></Message></Error>`
	})
	outcome, err = StopInstance(context.Background(), f.client(), "inst1", time.Second)
	require.Error(t, err, "another 409 is a failed terminate")
	require.Equal(t, sqlconnect.StopError, outcome)

	outcome, err = StopInstance(context.Background(), f.client(), "", time.Second)
	require.Error(t, err)
	require.Equal(t, sqlconnect.StopError, outcome)
}

func TestUnsupported(t *testing.T) {
	f := newFakeODPS(t)
	db := f.db(t)
	_, err := db.Begin()
	require.True(t, errors.Is(err, sqlconnect.ErrNotSupported))
	_, err = db.Prepare("SELECT 1")
	require.True(t, errors.Is(err, sqlconnect.ErrNotSupported))
	require.Error(t, db.Ping(), "ping loads the project, which the fake does not serve")
}

func TestExecBindsArguments(t *testing.T) {
	f := newFakeODPS(t)
	db := f.db(t)
	type named string
	_, err := db.ExecContext(context.Background(), "INSERT INTO t VALUES (?, ?, ?, ?, '?')", "it's \\", -3, nil, named("x"))
	require.NoError(t, err)
	require.Equal(t, `INSERT INTO t VALUES ('it\'s \\', (-3), NULL, 'x', '?');`, f.snapshot().submits[0].Query)

	_, err = db.ExecContext(context.Background(), "SELECT ?", time.Now())
	require.Error(t, err, "time.Time is refused in v1")
	_, err = db.ExecContext(context.Background(), "SELECT ?, ?", 1)
	require.Error(t, err)
	_, err = db.ExecContext(context.Background(), "SELECT 1 FROM `\x00refused identifier\x00`")
	require.ErrorIs(t, err, ErrRefusedIdentifier)
	require.Len(t, f.snapshot().submits, 1, "refusals create no job")
}

func TestExecRefusesSetOfProtectedSettings(t *testing.T) {
	f := newFakeODPS(t)
	db := f.db(t)
	for _, sql := range []string{
		"SET odps.sql.type.system.odps2=false",
		"set ODPS.SQL.TIMEZONE = UTC",
		"SET odps.sql.allow.fullscan=true; SELECT 1",
		"SELECT 1; SET odps.default.schema=other;",
		"/* x */ SET odps.sql.validate.orderby.limit=true",
	} {
		_, err := db.ExecContext(WithScript(context.Background()), sql)
		require.ErrorIs(t, err, ErrProtectedSet, sql)
	}
	require.Empty(t, f.snapshot().submits, "no job is created")
	_, err := db.ExecContext(WithScript(context.Background()), "SET odps.sql.mapper.split.size=64; SELECT 1")
	require.NoError(t, err, "other settings are the caller's business")
}

func TestStopOutcomeOnTheError(t *testing.T) {
	f := newFakeODPS(t)
	f.set(func(f *fakeODPS) { f.statuses = []string{"Running"} })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := f.db(t).ExecContext(ctx, "SELECT 1")
	var e *mcerr.Error
	require.ErrorAs(t, err, &e)
	require.Equal(t, string(sqlconnect.StopLanded), e.Stop)
	require.Equal(t, "inst1", e.InstanceID)

	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	_, err = f.db(t).ExecContext(cctx, "SELECT 1")
	require.ErrorIs(t, err, context.Canceled, "database/sql refuses a cancelled context before the driver sees it")
}

func TestShortRecordIsAnError(t *testing.T) {
	r := newRows(nil, "inst1")
	ch := make(chan item, 1)
	ch <- item{record: data.Record{data.BigInt(1)}}
	close(ch)
	r.ranges = []chan item{ch}
	r.want = 1
	err := r.Next(make([]driver.Value, 2))
	require.ErrorContains(t, err, "1 values for 2 columns")
}

func TestJobTag(t *testing.T) {
	f := newFakeODPS(t)
	db := f.db(t)
	ctx, err := WithJobTag(context.Background(), "8f1c2a9e-0b7d-4c3e-9a1f-2d3e4f5a6b7c")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)
	s := f.snapshot().submits[0]
	require.Equal(t, "rudderstack-retl:8f1c2a9e-0b7d-4c3e-9a1f-2d3e4f5a6b7c", s.Hints["rudderstack.job.tag"])
	require.Equal(t, "rudderstack_retl", s.TaskName)

	for _, bad := range []string{"", "has space", "a'b", "select * from t", strings.Repeat("a", 65), "中文"} {
		_, err := WithJobTag(context.Background(), bad)
		require.ErrorIs(t, err, ErrJobTag, bad)
	}
	_, err = db.ExecContext(WithScript(context.Background()), "SET rudderstack.job.tag=other; SELECT 1")
	require.ErrorIs(t, err, ErrProtectedSet, "the tag cannot be overridden from SQL")
}

func TestResultCountsAreNotSupported(t *testing.T) {
	f := newFakeODPS(t)
	db := f.db(t)
	for _, ctx := range []context.Context{context.Background(), WithScript(context.Background())} {
		res, err := db.ExecContext(ctx, "INSERT INTO t SELECT 1")
		require.NoError(t, err)
		_, err = res.RowsAffected()
		require.ErrorIs(t, err, sqlconnect.ErrNotSupported, "a caller must not trust a count of 0")
		_, err = res.LastInsertId()
		require.ErrorIs(t, err, sqlconnect.ErrNotSupported)
	}
}

func TestTransportRetries(t *testing.T) {
	saved := retryBackoff
	retryBackoff = func(int) time.Duration { return time.Millisecond }
	defer func() { retryBackoff = saved }()

	t.Run("a dropped result read keeps a failed job FAILED", func(t *testing.T) {
		f := newFakeODPS(t)
		f.set(func(f *fakeODPS) {
			f.statuses = []string{"Failed"}
			f.failureText = "ODPS-0130131:[1,15] Table not found - table p.s.t cannot be resolved"
			f.resultFailures = 2
		})
		_, err := f.db(t).ExecContext(context.Background(), "SELECT * FROM s.t")
		var e *mcerr.Error
		require.ErrorAs(t, err, &e)
		require.Equal(t, mcerr.CategoryNotFound, e.Category, "not network: the job really failed")
		require.False(t, e.Retryable)
		f.mu.Lock()
		require.Equal(t, 3, f.results)
		f.mu.Unlock()
	})

	t.Run("retries are bounded", func(t *testing.T) {
		f := newFakeODPS(t)
		f.set(func(f *fakeODPS) {
			f.statuses = []string{"Failed"}
			f.resultFailures = 10
		})
		_, err := f.db(t).ExecContext(context.Background(), "SELECT 1")
		var e *mcerr.Error
		require.ErrorAs(t, err, &e)
		require.Equal(t, mcerr.CategoryNetwork, e.Category)
		f.mu.Lock()
		require.Equal(t, 1+transportRetries, f.results)
		f.mu.Unlock()
	})

	t.Run("a terminate whose answer is lost counts as landed on retry", func(t *testing.T) {
		f := newFakeODPS(t)
		f.set(func(f *fakeODPS) {
			f.statuses = []string{"Running"}
			f.terminateFailures = 1
		})
		outcome, err := StopInstance(context.Background(), f.client(), "inst1", time.Second)
		require.NoError(t, err)
		require.Contains(t, []sqlconnect.StopOutcome{sqlconnect.StopLanded, sqlconnect.StopMissed}, outcome)
		require.Equal(t, 2, f.snapshot().terminates)
	})

	t.Run("a bare EOF is retried", func(t *testing.T) {
		calls := 0
		err := retryCall(context.Background(), func() error {
			calls++
			if calls < 3 {
				return pkgerrors.WithStack(io.EOF)
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, calls)
	})

	t.Run("a non-transport error is not retried", func(t *testing.T) {
		calls := 0
		err := retryCall(context.Background(), func() error { calls++; return errors.New("ODPS-0130131: x") })
		require.Error(t, err)
		require.Equal(t, 1, calls)
	})
}

func TestSubmitTimeouts(t *testing.T) {
	submitClient := func(f *fakeODPS, timeout time.Duration) func() (*odps.Odps, error) {
		return func() (*odps.Odps, error) {
			o := f.client()
			o.SetHttpTimeout(timeout)
			return o, nil
		}
	}

	t.Run("a submit HTTP timeout is a network error with an unknown outcome, not a cancel", func(t *testing.T) {
		f := newFakeODPS(t)
		f.set(func(f *fakeODPS) { f.submitDelay = 600 * time.Millisecond })
		db := f.db(t, func(c *Config) { c.NewSubmitClient = submitClient(f, 150*time.Millisecond) })
		_, err := db.ExecContext(context.Background(), "SELECT 1")
		var e *mcerr.Error
		require.ErrorAs(t, err, &e)
		require.Equal(t, mcerr.CategoryNetwork, e.Category, err.Error())
		require.True(t, e.OutcomeUnknown, "the server may have created the job")
		require.False(t, errors.Is(err, context.DeadlineExceeded), "the caller's context did not end")
		require.Empty(t, e.Stop)
	})

	t.Run("the caller's own deadline is still a cancel", func(t *testing.T) {
		f := newFakeODPS(t)
		f.set(func(f *fakeODPS) { f.submitDelay = 600 * time.Millisecond })
		db := f.db(t, func(c *Config) { c.NewSubmitClient = submitClient(f, 5*time.Second) })
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		_, err := db.ExecContext(ctx, "SELECT 1")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		var e *mcerr.Error
		require.ErrorAs(t, err, &e)
		require.Equal(t, mcerr.CategoryCancelled, e.Category)
		require.True(t, e.OutcomeUnknown)
		require.Eventually(t, func() bool { return f.snapshot().terminates == 1 }, 3*time.Second, 10*time.Millisecond, "the job the submit created is terminated")
	})

	t.Run("polls after a submit use the main client", func(t *testing.T) {
		f := newFakeODPS(t)
		f.set(func(f *fakeODPS) { f.statuses = []string{"Running", "Success"} })
		db := f.db(t, func(c *Config) { c.NewSubmitClient = submitClient(f, 5*time.Second) })
		var ids []string
		_, err := db.ExecContext(WithInstanceIDHandler(context.Background(), func(id string) { ids = append(ids, id) }), "SELECT 1")
		require.NoError(t, err)
		require.Equal(t, []string{"inst1"}, ids)
		require.Equal(t, 2, f.snapshot().polls)
	})

	t.Run("the SDK's 409 loop retries the submit, bounded by the caller's context", func(t *testing.T) {
		f := newFakeODPS(t)
		f.set(func(f *fakeODPS) { f.conflicts = 1 })
		_, err := f.db(t).ExecContext(context.Background(), "SELECT 1")
		require.NoError(t, err, "one 409 with Retry-After: 1, then created")
		require.Len(t, f.snapshot().submits, 2)

		f2 := newFakeODPS(t)
		f2.set(func(f *fakeODPS) { f.conflicts = 100 })
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err = f2.db(t).ExecContext(ctx, "SELECT 1")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, time.Since(start), 2*time.Second, "the context, not the SDK's 180 s loop, bounds the wait")
	})
}

func TestSubmitClientIsReplacedAfterIdle(t *testing.T) {
	f := newFakeODPS(t)
	made := 0
	cfg := Config{
		Client: f.client(), PollInitial: 5 * time.Millisecond, PollMax: 10 * time.Millisecond, TerminateWait: time.Second,
		NewSubmitClient: func() (*odps.Odps, error) { made++; return f.client(), nil },
	}
	connector, err := NewConnector(cfg)
	require.NoError(t, err)
	c := connector.(*Connector)
	clock := time.Unix(1_800_000_000, 0)
	c.now = func() time.Time { return clock }
	db := sql.OpenDB(c)
	defer func() { _ = db.Close() }()

	exec := func() {
		_, err := db.ExecContext(context.Background(), "SELECT 1")
		require.NoError(t, err)
	}
	exec()
	first := c.submitClient
	require.Equal(t, 1, made)

	clock = clock.Add(19 * time.Second)
	exec()
	require.Equal(t, 1, made, "a client used within 20 s is kept")
	require.Same(t, first, c.submitClient)

	clock = clock.Add(21 * time.Second)
	exec()
	require.Equal(t, 2, made, "a client idle for more than 20 s is replaced, with a new transport")
	require.NotSame(t, first, c.submitClient)

	f.set(func(f *fakeODPS) { f.statuses = []string{"Success"} })
	require.Len(t, f.snapshot().submits, 3)
}

func TestSubmitClientError(t *testing.T) {
	f := newFakeODPS(t)
	db := f.db(t, func(c *Config) {
		c.NewSubmitClient = func() (*odps.Odps, error) { return nil, errors.New("no client") }
	})
	_, err := db.ExecContext(context.Background(), "SELECT 1")
	var e *mcerr.Error
	require.ErrorAs(t, err, &e)
	require.Equal(t, mcerr.CategoryInternal, e.Category)
	require.Empty(t, f.snapshot().submits)
}

func TestSubmitClientNil(t *testing.T) {
	f := newFakeODPS(t)
	db := f.db(t, func(c *Config) {
		c.NewSubmitClient = func() (*odps.Odps, error) { return nil, nil } //nolint:nilnil // the case under test
	})
	_, err := db.ExecContext(context.Background(), "SELECT 1")
	var e *mcerr.Error
	require.ErrorAs(t, err, &e, "an error, not a panic")
	require.Equal(t, mcerr.CategoryInternal, e.Category)
}
