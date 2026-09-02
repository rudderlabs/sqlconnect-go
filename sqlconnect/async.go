package sqlconnect

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"time"

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
//
// Non-finite floats (NaN, +Inf, -Inf) are emitted as JSON null, at any depth, because JSON cannot
// represent them and json.Marshal would otherwise fail the entire result set rather than the one
// offending cell. The substitution is silent and lossy in one specific way: in the emitted JSON a
// non-finite value is indistinguishable from a real SQL NULL, so a caller deriving null-rate style
// metrics from this output will count such rows as unpopulated. Use QueryJSONMapAsync, which does
// not perform the substitution, when the distinction matters.
func QueryJSONAsync(ctx context.Context, db JsonQueryDB, query string, params ...any) (ch <-chan ValueOrError[json.RawMessage], leave func()) {
	jsonRowMapper := db.JSONRowMapper()
	mapper := func(cols []*sql.ColumnType, row RowScan) (json.RawMessage, error) {
		m, err := jsonRowMapper(cols, row)
		if err != nil {
			return nil, err
		}
		nullifyNonFiniteFloats(m)
		b, err := json.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("marshalling rows to json: %w", err)
		}
		return b, nil
	}
	return QueryAsync[json.RawMessage](ctx, db, mapper, query, params...)
}

// nullifyNonFiniteFloats replaces IEEE-754 NaN/±Inf with nil, in place, at any depth.
//
// JSON has no representation for them, so json.Marshal returns "unsupported value: NaN" — and
// because QueryAsync returns on the first mapper error, a single such cell fails the *entire*
// result set rather than one row or one column. Warehouses do store these values (Snowflake FLOAT
// accepts 'NaN'), so a query that merely reads user data could not be served at all.
//
// null is the only lossless-in-shape option: the column stays JSON-null rather than changing type
// to a "NaN" string, which would break consumers that parse the column as a number. This mirrors
// the undefined-in-ARRAY → null normalisation the snowflake mapper already performs. Note that this
// makes NaN, +Inf, -Inf and a real SQL NULL indistinguishable in the emitted JSON; that is inherent
// to encoding them as null, and is documented on QueryJSONAsync.
//
// Containers are walked because json.Marshal rejects a non-finite float at any depth, so handling
// only top-level cells would leave the same total failure reachable one level down — e.g. BigQuery
// `SELECT [IEEE_DIVIDE(0,0)]`, whose cell arrives as a []bigquery.Value (bigquery.Value is
// interface{}, so a plain []any case would not match it) and is passed through verbatim by the
// bigquery mapper's default branch, or a Trino array(double) containing nan().
//
// Deliberately confined to the JSON-bytes path: QueryJSONMapAsync hands callers the native
// map[string]any, where a float64 NaN is representable and meaningful, so it is left untouched.
func nullifyNonFiniteFloats(m map[string]any) {
	for k, v := range m {
		m[k] = nullifyNonFiniteValue(v, 0)
	}
}

// maxNonFiniteWalkDepth bounds the walk. Row values decoded from a warehouse are trees, not
// graphs, so this is a backstop against a pathological driver value rather than an expected case;
// beyond it the value is returned untouched and json.Marshal reports the error as it did before.
const maxNonFiniteWalkDepth = 32

// nullifyNonFiniteValue returns v with every non-finite float replaced by nil, mutating containers
// in place. The map it is called on belongs to a single row, freshly built by the row mapper, so
// in-place mutation cannot be observed by anything else.
func nullifyNonFiniteValue(v any, depth int) any {
	if depth > maxNonFiniteWalkDepth {
		return v
	}
	switch t := v.(type) {
	case nil, string, bool, []byte, json.RawMessage,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		time.Time:
		// The overwhelmingly common cells. Listed so they never reach the reflect fallback below.
		return v
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return nil
		}
		return v
	case float32:
		if f := float64(t); math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return v
	case map[string]any:
		for k, e := range t {
			t[k] = nullifyNonFiniteValue(e, depth+1)
		}
		return t
	case []any:
		for i, e := range t {
			t[i] = nullifyNonFiniteValue(e, depth+1)
		}
		return t
	}
	return nullifyNonFiniteNamedContainer(v, depth)
}

// nullifyNonFiniteNamedContainer handles containers whose named type a type switch cannot enumerate
// — []bigquery.Value being the motivating one. Only interface-element slices, arrays and maps are
// walked: those are the shapes a driver uses to carry an arbitrary cell, and their elements are
// settable, so a non-finite leaf can actually be replaced by nil. A concrete []float64 is left
// alone on purpose — it has no slot for a JSON null without changing the slice's type.
func nullifyNonFiniteNamedContainer(v any, depth int) any {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() != reflect.Interface {
			return v
		}
		if rv.Kind() == reflect.Array && !rv.CanSet() {
			return v // an array reached by value is a copy; mutating it would be a no-op
		}
		for i := range rv.Len() {
			setOrZero(rv.Index(i), nullifyNonFiniteValue(rv.Index(i).Interface(), depth+1))
		}
	case reflect.Map:
		if rv.Type().Elem().Kind() != reflect.Interface {
			return v
		}
		for _, k := range rv.MapKeys() {
			nv := nullifyNonFiniteValue(rv.MapIndex(k).Interface(), depth+1)
			if nv == nil {
				rv.SetMapIndex(k, reflect.Zero(rv.Type().Elem()))
				continue
			}
			rv.SetMapIndex(k, reflect.ValueOf(nv))
		}
	}
	return v
}

func setOrZero(dst reflect.Value, v any) {
	if v == nil {
		dst.Set(reflect.Zero(dst.Type()))
		return
	}
	dst.Set(reflect.ValueOf(v))
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
	return ch, leave
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
