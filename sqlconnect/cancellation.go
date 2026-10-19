package sqlconnect

import "context"

type KillRow struct {
	QueryID    string
	KillStatus string
}

type KillResult struct {
	Rows []KillRow
}

type QueryOutcome string

const (
	QueryRunning  QueryOutcome = "running"
	QueryFinished QueryOutcome = "finished"
	QueryFailed   QueryOutcome = "failed"
	QueryNotFound QueryOutcome = "not_found"
)

// QueryKind names the kind of a statement, such as "Insert", "Create",
// "Rename", "Alter" or "Drop".
type QueryKind string

// QueryController checks and stops the connector's own queries by query id.
// It reads no system table, so it needs no grant beyond the connector's own
// login. Both methods act on the replica that answers. StopQuery never stops a
// query of another user. Query ids must be unique per execution: reusing an id
// lets a stop reach a later query.
type QueryController interface {
	// QueryRunning reports whether a query runs under queryID. The server also
	// reports an id that another user's query holds as taken, so true means the
	// id is in use. The caller owns its query ids, so in practice true means
	// its own query runs. Each check sees only the replica that answers. On a
	// multi-replica service, false means "not running on the replica that answered".
	QueryRunning(ctx context.Context, queryID string) (bool, error)
	// StopQuery stops the connector's own query with queryID and confirms that
	// it no longer runs on the answering replica. stopped=true means the target
	// ran at the first check and does not run on the answering replica now; it
	// may have finished on its own between the check and the stop. stopped=false
	// means the first check found no running query. Callers must verify outcomes
	// from their own tables. A query that does not stop in time returns an error.
	StopQuery(ctx context.Context, queryID string) (stopped bool, err error)
}

// QueryCanceller is the earlier form of [QueryController].
//
// Deprecated: use [QueryController]. The ClickHouse driver implements both
// methods on top of it: KillQuery stops the query and returns one row with
// kill_status "finished" when it stopped a running query, and none when no
// query ran. QueryOutcome returns [QueryRunning] while the query runs, and
// [QueryFailed] once it does not run. QueryFailed then means only that the
// statement has ended, with its outcome unproven. Callers must move to
// [QueryController] plus verification from their own tables before they upgrade.
type QueryCanceller interface {
	KillQuery(ctx context.Context, queryID string) (KillResult, error)
	// Deprecated: QueryFailed means only "ended, outcome unproven". Callers must
	// move to QueryController plus their own verification before they upgrade.
	QueryOutcome(ctx context.Context, queryID string, kind QueryKind) (QueryOutcome, error)
}
