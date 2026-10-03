package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

const (
	p07OldUUID  = "11111111-1111-4111-8111-111111111111"
	p07WantUUID = "22222222-2222-4222-8222-222222222222"
)

func TestP07VisibilityExpectedUUID(t *testing.T) {
	for _, tc := range []struct {
		name   string
		script []stubResult
	}{
		{"immediate_match", []stubResult{row(p07WantUUID)}},
		{"old_to_expected", []stubResult{row(p07OldUUID), row(p07WantUUID)}},
		{"absent_old_expected", []stubResult{noRow, row(p07OldUUID), noRow, row(p07WantUUID)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A read past the expected match fails immediately, bounding a mutant
			// that never accepts a non-empty UUID without a long timeout loop.
			ex := scripted(t, append(tc.script, failWith(&ch.Exception{Code: 497}))...)
			var checker sqlconnect.VisibilityChecker = unitDB(t)
			got, err := checker.AwaitTable(context.Background(), ex,
				sqlconnect.NewRelationRef("snapshot", sqlconnect.WithSchema("scratch")), p07WantUUID,
				sqlconnect.VisibilityPolicy{InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, Deadline: 5 * time.Second})
			require.NoError(t, err)
			require.Equal(t, p07WantUUID, got)
			require.Equal(t, len(tc.script), ex.calls, "stop at the first matching UUID, never at the old table")
			require.Equal(t, []any{"scratch", "snapshot"}, ex.lastArgs)
		})
	}
}

// The model accepts the first visible row matching the requested identity,
// or any visible row for an empty request. Permission errors terminate it.
// The terminal match/error bounds every generated sequence and mutant run.
func FuzzP07VisibilitySequence(f *testing.F) {
	f.Add([]byte{}, false)
	f.Add([]byte{0, 1, 0, 1}, false)
	f.Add([]byte{1, 2, 1}, false)
	f.Add([]byte{0, 1, 2}, true)
	f.Add([]byte{1, 3, 2}, false)
	f.Fuzz(func(t *testing.T, observations []byte, anyUUID bool) {
		if len(observations) > 32 {
			observations = observations[:32]
		}
		want := p07WantUUID
		if anyUUID {
			want = ""
		}
		script := make([]stubResult, 0, len(observations)+2)
		for _, observation := range observations {
			switch observation % 4 {
			case 0:
				script = append(script, noRow)
			case 1:
				script = append(script, row(p07OldUUID))
			case 2:
				script = append(script, row(p07WantUUID))
			case 3:
				script = append(script, failWith(&ch.Exception{Code: 497}))
			}
		}
		script = append(script, row(p07WantUUID), failWith(&ch.Exception{Code: 497}))
		var expected string
		var denied bool
		var calls int
		for _, answer := range script {
			calls++
			if answer.err != nil {
				denied = true
				break
			}
			if len(answer.rows) == 0 {
				continue
			}
			if anyUUID || answer.rows[0] == p07WantUUID {
				expected = answer.rows[0]
				break
			}
		}
		ex := scripted(t, script...)
		got, err := unitDB(t).AwaitTable(context.Background(), ex, sqlconnect.NewRelationRef("snapshot"), want,
			sqlconnect.VisibilityPolicy{InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond, Deadline: 5 * time.Second})
		if denied {
			requireCode(t, err, "CH_PERMISSION")
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, expected, got)
		require.Equal(t, calls, ex.calls)
	})
}

// Check policy normalization without polling: a deadline regression should
// fail immediately, rather than forcing the suite to wait for that deadline.
func TestP07VisibilityPolicyDefaults(t *testing.T) {
	defaults := sqlconnect.VisibilityPolicy{
		InitialBackoff: 250 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Deadline:       60 * time.Second,
	}
	require.Equal(t, defaults, withDefaults(sqlconnect.VisibilityPolicy{}))
	require.Equal(t, defaults, withDefaults(sqlconnect.VisibilityPolicy{
		InitialBackoff: -time.Second, MaxBackoff: -time.Second, Deadline: -time.Second,
	}))
	custom := sqlconnect.VisibilityPolicy{
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, Deadline: 3 * time.Second,
	}
	require.Equal(t, custom, withDefaults(custom), "positive caller budgets must survive normalization")
}
