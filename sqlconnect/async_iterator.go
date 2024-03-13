package sqlconnect

import (
	"context"
	"encoding/json"
)

// AsyncIterator is a helper for iterating over the results of a query asynchronously.
//
// Deprecated: use [sqlconnect.QueryJSONAsync] instead
type AsyncIterator struct {
	DB    JsonQueryDB
	Query string

	rows  <-chan ValueOrError[json.RawMessage]
	leave func()
	err   error
}

// Start the async iteration
func (i *AsyncIterator) Start(ctx context.Context) {
	i.rows, i.leave = QueryJSONAsync(ctx, i.DB, i.Query)
}

// Next will fetch the next row or [nil] if there are no more rows or an error occurred. In such a case, use [Error] to check for errors.
func (i *AsyncIterator) Next() *json.RawMessage {
	row, ok := <-i.rows
	if !ok { // channel closed
		i.leave()
		return nil
	}
	if row.Err != nil { // error
		i.err = row.Err
		i.leave()
		return nil
	}
	return &row.Value
}

// Error will return an error if such was encountered during the iteration.
func (i *AsyncIterator) Error() error {
	return i.err
}

// Stop cancels the async iteration. If the iteration is already finished, this is a no-op. Can be called multiple times safely
func (i *AsyncIterator) Stop() {
	i.leave()
}
