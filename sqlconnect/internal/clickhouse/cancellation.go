package clickhouse

import (
	"context"
	"database/sql"
	"errors"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chctx"
)

var _ sqlconnect.QueryCanceller = (*DB)(nil)

// Every statement here filters on user = currentUser(), so a connector never
// stops or reads the queries of another user. ON CLUSTER is never sent: the
// driver supports single-replica and Cloud services only.
const (
	killSQL      = "KILL QUERY WHERE query_id = ? AND user = currentUser() SYNC"
	processesSQL = "SELECT count()\nFROM system.processes\nWHERE query_id = ? AND user = currentUser()"
	queryLogSQL  = "SELECT type\nFROM system.query_log\nWHERE query_id = ?\n  AND user = currentUser()\n  AND query_kind = ?\n" +
		"  AND type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')\n" +
		"  AND event_date >= today() - 1\nORDER BY event_time_microseconds DESC\nLIMIT 1"
)

// controlCtx keeps the caller's settings map, never the caller's query id. A
// control statement under the target's id would collide with the target on
// the server, and its own query_log row would count as the target's outcome.
func (db *DB) controlCtx(ctx context.Context) context.Context {
	fresh := clickhousequery.NewQueryID()
	if chctx.Has(ctx) {
		return ch.Context(ctx, ch.WithQueryID(fresh))
	}
	return clickhousequery.WithStatement(ctx, controlSettings(), fresh)
}

// controlPool returns the control pool. A DB that a unit test builds by hand
// has none and uses the main pool.
func (db *DB) controlPool() *sql.DB {
	if db.control != nil {
		return db.control
	}
	return db.DB.DB
}

// KillQuery implements sqlconnect.QueryCanceller. It returns every kill row
// and interprets none; only the caller decides what a kill_status means.
func (db *DB) KillQuery(ctx context.Context, queryID string) (sqlconnect.KillResult, error) {
	rows, err := db.controlPool().QueryContext(db.controlCtx(ctx), killSQL, queryID)
	if err != nil {
		return sqlconnect.KillResult{}, bound("kill query", "", err)
	}
	defer func() { _ = rows.Close() }()
	names, err := rows.Columns()
	if err != nil {
		return sqlconnect.KillResult{}, bound("kill query", "", err)
	}
	var res sqlconnect.KillResult
	for rows.Next() {
		var r sqlconnect.KillRow
		dest := make([]any, len(names))
		for i, n := range names {
			switch n {
			case "query_id":
				dest[i] = &r.QueryID
			case "kill_status":
				dest[i] = &r.KillStatus
			default:
				dest[i] = new(sqlconnect.NilAny)
			}
		}
		if err := rows.Scan(dest...); err != nil {
			return sqlconnect.KillResult{}, bound("kill query", "", err)
		}
		res.Rows = append(res.Rows, r)
	}
	if err := rows.Err(); err != nil {
		return sqlconnect.KillResult{}, bound("kill query", "", err)
	}
	return res, nil
}

// QueryOutcome implements sqlconnect.QueryCanceller.
func (db *DB) QueryOutcome(ctx context.Context, queryID string, kind sqlconnect.QueryKind) (sqlconnect.QueryOutcome, error) {
	return queryOutcome(func(q string, args ...any) *sql.Row {
		return db.controlPool().QueryRowContext(db.controlCtx(ctx), q, args...)
	}, queryID, kind)
}

// queryOutcome reads system.processes, then system.query_log, through row.
func queryOutcome(row func(q string, args ...any) *sql.Row, queryID string, kind sqlconnect.QueryKind) (sqlconnect.QueryOutcome, error) {
	var running uint64
	if err := row(processesSQL, queryID).Scan(&running); err != nil {
		return "", bound("query outcome", "", err)
	}
	if running > 0 {
		return sqlconnect.QueryRunning, nil
	}
	var typ string
	err := row(queryLogSQL, queryID, string(kind)).Scan(&typ)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return sqlconnect.QueryNotFound, nil
	case err != nil:
		return "", bound("query outcome", "", err)
	case typ == "QueryFinish":
		return sqlconnect.QueryFinished, nil
	default:
		return sqlconnect.QueryFailed, nil
	}
}
