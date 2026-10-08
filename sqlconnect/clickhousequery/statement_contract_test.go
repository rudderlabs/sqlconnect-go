package clickhousequery

import (
	"context"
	"reflect"
	"testing"
	"time"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chctx"
)

// The fork exposes no settings getter. Probe the same QueryOptions handed to
// the driver, without unsafe or changing the options (as queryIDOf does in the
// driver's cancellation tests).
func p12StatementOptions(t *testing.T, ctx context.Context) (map[string]any, string) {
	t.Helper()
	m := map[string]any{}
	var id string
	_ = ch.Context(ctx, func(o *ch.QueryOptions) error {
		v := reflect.ValueOf(o).Elem()
		id = v.FieldByName("queryID").String()
		it := v.FieldByName("settings").MapRange()
		for it.Next() {
			value := it.Value().Elem()
			switch value.Kind() {
			case reflect.Int:
				m[it.Key().String()] = int(value.Int())
			case reflect.String:
				m[it.Key().String()] = value.String()
			default:
				t.Fatalf("unexpected setting type %s", value.Kind())
			}
		}
		return nil
	})
	return m, id
}

func TestStatementCopiesReplacesAndMarks(t *testing.T) {
	parent := WithStatement(context.Background(), map[string]any{"readonly": 0, "limit": 99}, "parent")
	input := map[string]any{"readonly": 2, "join_use_nulls": 1, "read_overflow_mode": "throw"}
	child := WithStatement(parent, input, "child")
	input["readonly"] = 0
	delete(input, "join_use_nulls")
	input["offset"] = 50
	want := map[string]any{"readonly": 2, "join_use_nulls": 1, "read_overflow_mode": "throw"}
	if got, id := p12StatementOptions(t, child); !reflect.DeepEqual(want, got) || id != "child" {
		t.Fatalf("child = %#v, %q; want %#v, child", got, id, want)
	}
	if !chctx.Has(child) {
		t.Fatal("statement lacks the marker that prevents driver defaults")
	}
	if got, id := p12StatementOptions(t, parent); !reflect.DeepEqual(map[string]any{"readonly": 0, "limit": 99}, got) || id != "parent" {
		t.Fatalf("child changed its parent: %#v, %q", got, id)
	}
	for _, empty := range []map[string]any{nil, {}} {
		ctx := WithStatement(parent, empty, "empty")
		got, id := p12StatementOptions(t, ctx)
		if len(got) != 0 || id != "empty" || !chctx.Has(ctx) {
			t.Fatalf("empty map must replace and mark: %#v, %q", got, id)
		}
	}
}

func FuzzStatementIsolation(f *testing.F) {
	f.Add("readonly", 2)
	f.Add("limit", 0)
	f.Add("join_use_nulls", 1)
	f.Fuzz(func(t *testing.T, key string, value int) {
		input := map[string]any{key: value}
		deadline := time.Now().Add(time.Hour)
		parent, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()
		ctx := WithStatement(parent, input, "retl-property")
		clear(input)
		got, id := p12StatementOptions(t, ctx)
		if !reflect.DeepEqual(map[string]any{key: value}, got) || id != "retl-property" || !chctx.Has(ctx) {
			t.Fatalf("statement lost its snapshot: %#v, %q", got, id)
		}
		if d, ok := ctx.Deadline(); !ok || !d.Equal(deadline) {
			t.Fatal("statement lost the parent deadline")
		}
		cancel()
		if ctx.Err() != context.Canceled {
			t.Fatalf("statement lost parent cancellation: %v", ctx.Err())
		}
	})
}
