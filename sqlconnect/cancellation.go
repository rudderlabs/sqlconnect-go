package sqlconnect

import "context"

// StopOutcome is the result of [QueryController.StopQuery]. It has three
// values only, so a caller can use it as a metric label.
type StopOutcome string

const (
	// StopLanded means the query ran on the answering replica at the first
	// check and does not run there now. It may have ended on its own between
	// the check and the stop.
	StopLanded StopOutcome = "landed"
	// StopMissed means no query ran under the id on the answering replica. The
	// query has ended, or it runs on another replica of a multi-replica
	// service. A caller that still waits for the answer of its statement
	// knows that the stop missed it.
	StopMissed StopOutcome = "missed"
	// StopError means the stop failed. The error says why.
	StopError StopOutcome = "error"
)

// QueryController stops the connector's own write by query id. It reads no
// system table, so it needs no grant beyond the connector's own login. A read
// needs no stop: cancelling its context closes the request, and the server
// stops a read when its client goes away.
type QueryController interface {
	// StopQuery stops the connector's own query with queryID on the replica
	// that answers. It never stops a query of another user. Query ids must be
	// unique per execution: a reused id lets a stop reach a later query. The
	// outcome is [StopError] exactly when err is not nil. Callers must verify
	// what a stopped write left behind from their own tables.
	StopQuery(ctx context.Context, queryID string) (StopOutcome, error)
}
