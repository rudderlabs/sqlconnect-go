package clickhouse

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

// requireCode asserts that err carries an adapter error with the given code.
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var ce *cherr.Error
	require.True(t, errors.As(err, &ce), "want %s, got %v", code, err)
	require.Equal(t, code, ce.Code, "%v", err)
}

// testWorkingDB is the working database the unit stubs answer for.
const testWorkingDB = "_rudderstack"

// workingCtx returns ctx with the validation options of ctx and testWorkingDB.
func workingCtx(ctx context.Context) context.Context {
	o, _ := sqlconnect.ValidationOptionsFrom(ctx)
	o.WorkingDatabase = testWorkingDB
	return sqlconnect.WithValidationOptions(ctx, o)
}
