package clickhouse

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

// requireCode asserts that err carries an adapter error with the given code.
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var ce *cherr.Error
	require.True(t, errors.As(err, &ce), "want %s, got %v", code, err)
	require.Equal(t, code, ce.Code, "%v", err)
}
