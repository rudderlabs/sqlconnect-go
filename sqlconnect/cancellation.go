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
	// query has ended, or it runs on another replica of a multi-replica
	// service. A caller that still waits for the answer of its statement
	// knows that the stop missed it.
	StopMissed StopOutcome = "missed"
	// StopError means the stop failed. The error says why.
	StopError StopOutcome = "error"
)

// QueryController stops the connector's own write by query id. It reads no
// system table, so it needs no grant beyond the connector's own login. A read
// needs no stop: cancelling its context closes the request, and a read sent
// with readonly > 0 and cancel_http_readonly_queries_on_client_close = 1 stops
// when its client goes away. A server that ends TLS itself sees the close only
// when it next writes output. A read with little output then runs on until
// its max_execution_time.
type QueryController interface {
	// StopQuery stops the connector's own query with queryID on the replica
	// that answers. It never stops a query of another user. Query ids must be
	// unique per execution: a reused id lets a stop reach a later query. The
	// outcome is [StopError] exactly when err is not nil. Callers must verify
	// what a stopped write left behind from their own tables.
	StopQuery(ctx context.Context, queryID string) (StopOutcome, error)
}
