package clickhouse

import (
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chctx"
)

func p12SettingsAt(t *testing.T, ctx context.Context) map[string]any {
	t.Helper()
	m := map[string]any{}
	_ = ch.Context(ctx, func(o *ch.QueryOptions) error {
		it := reflect.ValueOf(o).Elem().FieldByName("settings").MapRange()
		for it.Next() {
			v := it.Value().Elem()
			switch v.Kind() {
			case reflect.Int:
				m[it.Key().String()] = int(v.Int())
			case reflect.String:
				m[it.Key().String()] = v.String()
			default:
				t.Fatalf("unexpected settings type %s", v.Kind())
			}
		}
		return nil
	})
	return m
}

func TestDriverConnectionReceivesSettings(t *testing.T) {
	stub := &recordingConn{rows: &errRows{err: io.EOF}}
	g := &guardConn{inner: stub}
	_, err := g.ExecContext(context.Background(), "INSERT INTO t SELECT 1", nil)
	require.NoError(t, err)
	require.True(t, chctx.Has(stub.lastCtx))
	require.Equal(t, p12ScratchWant(), p12SettingsAt(t, stub.lastCtx))
	rows, err := g.QueryContext(context.Background(), "SELECT 1", nil)
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	require.True(t, chctx.Has(stub.lastCtx))
	require.Equal(t, p12ReadWant(), p12SettingsAt(t, stub.lastCtx))
	for _, input := range []map[string]any{nil, {}, {"readonly": 2, "limit": 7}} {
		ctx := clickhousequery.WithStatement(context.Background(), input, "retl-p12")
		want := map[string]any{}
		for k, v := range input {
			want[k] = v
		}
		_, err = g.ExecContext(ctx, "SELECT 1", nil)
		require.NoError(t, err)
		require.Equal(t, want, p12SettingsAt(t, stub.lastCtx), "explicit maps replace all driver defaults")
		rows, err = g.QueryContext(ctx, "SELECT 1", nil)
		require.NoError(t, err)
		require.NoError(t, rows.Close())
		require.Equal(t, want, p12SettingsAt(t, stub.lastCtx))
	}
}
