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

// QueryKind is a system.query_log.query_kind value, such as "Insert",
// "Create", "Rename", "Alter" or "Drop".
type QueryKind string

type QueryCanceller interface {
	// KillQuery runs KILL QUERY ... SYNC for queryID on a pooled connection
	// and returns the kill rows. It runs under its own fresh query id.
	KillQuery(ctx context.Context, queryID string) (KillResult, error)
	// QueryOutcome reads system.processes, then system.query_log, for queryID
	// on the replica that answers. It reads only rows of the given kind and
	// of the connector's own user. It runs under its own fresh query id.
	// The target statement must have log_queries = 1 to record its completed
	// outcome. Callers supplying their own settings map must enable it there;
	// enabling it on this lookup cannot recover an unlogged target statement.
	QueryOutcome(ctx context.Context, queryID string, kind QueryKind) (QueryOutcome, error)
}
