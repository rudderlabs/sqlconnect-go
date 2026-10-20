package clickhousequery

import (
	"context"
	"maps"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chctx"
)

// WithStatement returns a context that sends settings and queryID with the
// next statement. It copies settings, so a later change by the caller has no
// effect. The map replaces any map on the parent context, and the driver then
// adds none of its own defaults. A read map must set readonly > 0 and
// cancel_http_readonly_queries_on_client_close = 1, so the server stops the
// read when its context ends.
func WithStatement(ctx context.Context, settings map[string]any, queryID string) context.Context {
	m := make(ch.Settings, len(settings))
	maps.Copy(m, settings)
	return chctx.Mark(ch.Context(ctx, ch.WithSettings(m), ch.WithQueryID(queryID)))
}
