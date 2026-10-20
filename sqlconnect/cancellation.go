package sqlconnect

import "context"

// StopOutcome is the result of [QueryController.StopQuery]. It has three
// values only, so a caller can use it as a metric label.
type StopOutcome string

const (
	// StopLanded means the first check saw the query, and the stop and its
	// confirmation succeeded. The query may have ended on its own in between.
	// On a multi-replica service the stop can reach a replica that does not
	// run the query, so only the query's own error proves the stop: the
	// server ends a stopped query with code 394 (CH_CANCELLED).
	StopLanded StopOutcome = "landed"
	// StopMissed means no query ran under the id on the answering replica. The
	// query has ended, has not reached the server yet, or runs on another
	// replica of a multi-replica service. A caller that still waits for the answer of its statement
	// knows that the stop missed it.
	StopMissed StopOutcome = "missed"
	// StopError means the stop outcome is unknown. The server may still stop
	// the query. The error says why.
	StopError StopOutcome = "error"
)

// QueryController stops a query of the connector's own user by query id. It
// reads no system table, so it needs no grant beyond the connector's own login.
// Cancelling a read's context closes the request. A read sent with readonly > 0
// and cancel_http_readonly_queries_on_client_close = 1 then stops at its next
// input read after the client goes away, or at its max_execution_time. A
// CPU-bound read, such as a large join, can run on for many seconds. A server
// that ends TLS itself sees the close only when it next writes output.
// StopQuery stops a read by its id too.
type QueryController interface {
	// StopQuery stops the connector's own query with queryID on the replica
	// that answers. It never stops a query of another user. Query ids must be
	// unique per execution: a reused id lets a stop reach a later query. The
	// outcome is [StopError] exactly when err is not nil. Callers must verify
	// what a stopped write left behind from their own tables.
	StopQuery(ctx context.Context, queryID string) (StopOutcome, error)
}
