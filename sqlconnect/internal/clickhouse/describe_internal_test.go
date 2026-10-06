package clickhouse

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestPoolExec_RefusesWrites(t *testing.T) {
	_, err := poolExec{}.ExecContext(context.Background(), "DROP TABLE t")
	requireCode(t, err, "CH_QUERY_INVALID")
	require.ErrorIs(t, err, sqlconnect.ErrNotSupported)
}
