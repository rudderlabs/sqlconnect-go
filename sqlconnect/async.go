package sqlconnect

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/async"
)

type JsonQueryDB interface {
	JsonRowMapper
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type QueryDB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// QueryJSONMapAsync executes a query and returns a channel that will receive the results as a map or an error, along with a function that the caller can use to leave the channel early.
// The channel will be closed when the query is done or when the context is canceled.
func QueryJSONMapAsync(ctx context.Context, db JsonQueryDB, query string, params ...any) (ch <-chan ValueOrError[map[string]any], leave func()) {
	return QueryAsync[map[string]any](ctx, db, db.JSONRowMapper(), query, params...)
}

// QueryJSONAsync executes a query and returns a channel that will receive the results as json or an error, along with a function that the caller can use to leave the channel early.
// The channel will be closed when the query is done or when the context is canceled.
func QueryJSONAsync(ctx context.Context, db JsonQueryDB, query string, params ...any) (ch <-chan ValueOrError[json.RawMessage], leave func()) {
	jsonRowMapper := db.JSONRowMapper()
	mapper := func(cols []*sql.ColumnType, row RowScan) (json.RawMessage, error) {
		m, err := jsonRowMapper(cols, row)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("marshalling rows to json: %w", err)
		}
		return b, nil
	}
	return QueryAsync[json.RawMessage](ctx, db, mapper, query, params...)
}

// QueryAsync executes a query and returns a channel that will receive the results or an error, along with a function that the caller can use to leave the channel early.
// The channel will be closed when the query is done or when the context is canceled.
func QueryAsync[T any](ctx context.Context, db QueryDB, mapper RowMapper[T], query string, params ...any) (ch <-chan ValueOrError[T], leave func()) {
	s := &async.SingleSender[ValueOrError[T]]{}
	ctx, ch, leave = s.Begin(ctx)
	go func() {
		defer s.Close()
		rows, err := db.QueryContext(ctx, query, params...)
		if err != nil {
			s.Send(ValueOrError[T]{Err: fmt.Errorf("executing query: %w", err)})
			return
		}
		defer func() { _ = rows.Close() }()
		cols, err := rows.ColumnTypes()
		if err != nil {
			s.Send(ValueOrError[T]{Err: fmt.Errorf("getting column types: %w", err)})
			return
		}
		for rows.Next() {
			select {
			case <-ctx.Done():
				s.Send(ValueOrError[T]{Err: ctx.Err()})
				return
			default:
			}
			v, err := mapper(cols, rows)
			if err != nil {
				s.Send(ValueOrError[T]{Err: fmt.Errorf("mapping row: %w", err)})
				return
			}
			s.Send(ValueOrError[T]{Value: v})
		}
		if err := rows.Err(); err != nil {
			s.Send(ValueOrError[T]{Err: fmt.Errorf("iterating rows: %w", err)})
		}
	}()
	return
}

// ValueOrError represents a value or an error
type ValueOrError[T any] struct {
	Value T
	Err   error
}

// RowScan is an interface that represents a row scanner
type RowScan interface {
	Scan(dest ...any) error
}

// RowMapper is a function that maps database rows to a value
type RowMapper[T any] func(cols []*sql.ColumnType, row RowScan) (T, error)

// JSONRowMapper returns a row mapper that scans rows and maps them to [map[string]any]
func JSONRowMapper(valueMapper func(databaseTypeName string, value any) any) RowMapper[map[string]any] {
	return func(cols []*sql.ColumnType, row RowScan) (map[string]any, error) {
		values := make([]any, len(cols))
		for i := range values {
			values[i] = new(NilAny)
		}
		if err := row.Scan(values...); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		o := map[string]any{}
		for i := range values {
			v := values[i].(*NilAny)
			var val any
			if v != nil {
				val = v.Value
				// copying bytes to avoid them being overwritten by the next row, since some drivers reuse the same buffer (e.g. postgres)
				if bytes, ok := val.([]byte); ok {
					bc := make([]byte, len(bytes))
					copy(bc, bytes)
					val = bc
				}
			}
			o[cols[i].Name()] = valueMapper(cols[i].DatabaseTypeName(), val)
		}
		return o, nil
	}
}

type NilAny struct {
	Value any
}

func (v *NilAny) Scan(src any) error {
	switch src.(type) {
	case nil:
		v.Value = nil
	default:
		v.Value = src
	}
	return nil
}
