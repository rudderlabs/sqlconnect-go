package clickhouse

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

var (
	_ sqlconnect.QueryController = (*DB)(nil)
	_ sqlconnect.QueryCanceller  = (*DB)(nil) //nolint:staticcheck // kept until rudder-sources moves to QueryController
)

// The driver never reads system.processes or system.query_log and never sends
// KILL QUERY: KILL needs SELECT ON system.processes even for the user's own
// query, and those tables show the query text of every user. Both statements
// below run under the target's query id instead. The stop replaces only a query
// of the same user; another user's query under that id answers 216 and keeps
// running. The check is not passive: it holds the id for one round trip, so a
// statement that starts under that id during a check fails with 216. Callers
// check only after their own statement was sent. ON CLUSTER is never sent: the driver supports single-replica and
// Cloud services only, and both statements act on the replica that answers.
// Query ids must be unique per execution: reusing one lets a stop reach a later
// query.
const (
	// runningSQL fails with 216 while a query with the same id runs.
	runningSQL = "SELECT 1 AS running_check"
	// stopSQL, sent with replace_running_query = 1, makes the server cancel the
	// running query with the same id before it runs. The server waits at most
	// replace_running_query_max_wait_ms (default 5 s) and then fails with 216.
	stopSQL = "SELECT 1 AS stop_own_query"
	// codeAlreadyRunning is QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING.
	codeAlreadyRunning   = 216
	codeQueryCancelled   = 394
	controlRetryInterval = 20 * time.Millisecond
)

var (
	// stopTimeout bounds control calls, including lock waits, unless the
	// caller's deadline is earlier.
	stopTimeout = 30 * time.Second
	// stopConfirmTries and stopConfirmInterval bound the running checks after
	// the stop statement returned.
	stopConfirmTries    = 3
	stopConfirmInterval = 200 * time.Millisecond
)

// targetCtx sends the hello map under the target's query id. It replaces any
// caller map. The hello map keeps a profile limit, offset or result filter
// from hiding the one row the check reads back. It pins
// replace_running_query to 0 for a check and to 1 for a stop, overriding the
// user's profile default.
func targetCtx(ctx context.Context, queryID string, replace bool) context.Context {
	m := helloSettings()
	m["replace_running_query"] = 0
	if replace {
		m["replace_running_query"] = 1
	}
	return clickhousequery.WithStatement(ctx, m, queryID)
}

// controlPool returns the control pool. A DB that a unit test builds by hand
// has none and uses the main pool.
func (db *DB) controlPool() *sql.DB {
	if db.control != nil {
		return db.control
	}
	return db.DB.DB
}

// controlLocks is process-wide: two DB handles to one service must not race
// each other's checks under the same target id.
var controlLocks keyedMutex

// keyedMutex serializes control calls per id. References include waiters,
// so an entry is removed only after the last holder or waiter leaves. A
// channel lets a caller's deadline also bound the wait for the lock.
type keyedMutex struct {
	mu      sync.Mutex
	entries map[string]*targetLock
}

type targetLock struct {
	token chan struct{}
	refs  int
}

func (m *keyedMutex) lock(ctx context.Context, id string) (func(), error) {
	m.mu.Lock()
	if m.entries == nil {
		m.entries = make(map[string]*targetLock)
	}
	entry := m.entries[id]
	if entry == nil {
		entry = &targetLock{token: make(chan struct{}, 1)}
		m.entries[id] = entry
	}
	entry.refs++
	m.mu.Unlock()
	releaseRef := func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		entry.refs--
		if entry.refs == 0 {
			delete(m.entries, id)
		}
	}
	select {
	case entry.token <- struct{}{}:
		return func() { <-entry.token; releaseRef() }, nil
	case <-ctx.Done():
		releaseRef()
		return nil, ctx.Err()
	}
}

// alreadyRunning reports the server answer 216 for a statement sent under a
// query id that a query of the same user still runs under.
func alreadyRunning(err error) bool { return classify(err).ServerCode == codeAlreadyRunning }

// QueryRunning implements sqlconnect.QueryController. Each check sees only
// the replica that answers; false on a multi-replica service means "not running
// on the replica that answered". Query ids must be unique per execution.
func (db *DB) QueryRunning(ctx context.Context, queryID string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, stopTimeout)
	defer cancel()
	if queryID == "" {
		return false, cherr.New(cherr.CodeConfigInvalid, "queryID", "the query id is required")
	}
	unlock, err := controlLocks.lock(ctx, queryID)
	if err != nil {
		return false, bound("query running", "", err)
	}
	defer unlock()
	return db.queryRunning(ctx, queryID)
}

// queryRunning checks while the caller holds the target's control lock.
func (db *DB) queryRunning(ctx context.Context, queryID string) (bool, error) {
	for attempt := range 2 {
		var one uint8
		err := db.controlPool().QueryRowContext(targetCtx(ctx, queryID, false), runningSQL).Scan(&one)
		switch {
		case err == nil:
			return false, nil
		case alreadyRunning(err):
			return true, nil
		case classify(err).ServerCode == codeQueryCancelled:
			if attempt == 0 {
				if err := sleep(ctx, controlRetryInterval); err != nil {
					return false, bound("query running", "", err)
				}
				continue
			}
			e := wrap(cherr.CodeOutcomeUnknown, "", "the running check remained contended", err)
			e.ServerCode = codeQueryCancelled
			return false, e
		default:
			return false, bound("query running", "", err)
		}
	}
	return false, cherr.New(cherr.CodeOutcomeUnknown, "", "the running check remained contended")
}

// StopQuery implements sqlconnect.QueryController. It checks first, so it
// sends the stop statement only for a query that runs, then confirms. Every
// step is bounded, and every failure is a classified error: a refused setting
// (a readonly profile or a constraint) is CH_PERMISSION, and a query that
// still runs after the stop is CH_OUTCOME_UNKNOWN. stopped=true means the
// target ran at the first check and does not run on the answering replica now.
// It may have finished on its own between the check and stop. Callers must
// verify outcomes from their own tables, and use a unique id per execution.
func (db *DB) StopQuery(ctx context.Context, queryID string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, stopTimeout)
	defer cancel()
	if queryID == "" {
		return false, cherr.New(cherr.CodeConfigInvalid, "queryID", "the query id is required")
	}
	unlock, err := controlLocks.lock(ctx, queryID)
	if err != nil {
		return false, bound("stop query", "", err)
	}
	defer unlock()
	running, err := db.queryRunning(ctx, queryID)
	if err != nil || !running {
		return false, err
	}
	var one uint8
	if err := db.controlPool().QueryRowContext(targetCtx(ctx, queryID, true), stopSQL).Scan(&one); err != nil {
		if alreadyRunning(err) {
			return false, cherr.New(cherr.CodeOutcomeUnknown, "", "the query did not stop in time")
		}
		if classify(err).ServerCode == codeQueryCancelled {
			e := wrap(cherr.CodeOutcomeUnknown, "", "the stop statement was cancelled by a contending query", err)
			e.ServerCode = codeQueryCancelled
			return false, e
		}
		return false, bound("stop query", "", err)
	}
	// The stop statement can stay registered under the id for a moment after
	// its response, so a 216 right after it is retried a bounded number of times.
	for i := range stopConfirmTries {
		running, err = db.queryRunning(ctx, queryID)
		if err != nil {
			return false, err
		}
		if !running {
			return true, nil
		}
		if i < stopConfirmTries-1 {
			if err := sleep(ctx, stopConfirmInterval); err != nil {
				return false, bound("stop query", "", err)
			}
		}
	}
	return false, cherr.New(cherr.CodeOutcomeUnknown, "", "the query still runs after the stop")
}

// KillQuery implements sqlconnect.QueryCanceller on top of StopQuery.
//
// Deprecated: use StopQuery.
func (db *DB) KillQuery(ctx context.Context, queryID string) (sqlconnect.KillResult, error) {
	stopped, err := db.StopQuery(ctx, queryID)
	if err != nil || !stopped {
		return sqlconnect.KillResult{}, err
	}
	return sqlconnect.KillResult{Rows: []sqlconnect.KillRow{{QueryID: queryID, KillStatus: "finished"}}}, nil
}

// QueryOutcome implements sqlconnect.QueryCanceller on top of QueryRunning.
// kind is ignored. QueryFailed means only "ended, outcome unproven".
//
// Deprecated: callers must move to QueryController plus verification from
// their own tables before they upgrade.
func (db *DB) QueryOutcome(ctx context.Context, queryID string, _ sqlconnect.QueryKind) (sqlconnect.QueryOutcome, error) {
	running, err := db.QueryRunning(ctx, queryID)
	switch {
	case err != nil:
		return "", err
	case running:
		return sqlconnect.QueryRunning, nil
	default:
		return sqlconnect.QueryFailed, nil
	}
}
