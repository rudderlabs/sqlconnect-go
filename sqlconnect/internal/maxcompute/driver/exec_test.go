package driver

import (
	"context"
	"errors"
	"testing"
	"time"

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
	}, s.submits[0].Hints, "the fixed map and the default schema; never a time zone")
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
	_, err = db.ExecContext(context.Background(), "SELECT ?", 1)
	require.Error(t, err)
}
