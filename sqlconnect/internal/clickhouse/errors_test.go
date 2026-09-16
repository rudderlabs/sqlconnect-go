package clickhouse

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"testing"

	ch "github.com/rudderlabs/clickhouse-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestClassifyError(t *testing.T) {
	t.Parallel()
	db := &DB{}
	for _, tt := range []struct {
		err    error
		code   string
		server int32
	}{
		{fmt.Errorf("wrapped: %w", context.Canceled), "CH_CANCELLED", 0},
		{context.DeadlineExceeded, "CH_TIMEOUT", 0},
		{fmt.Errorf("wrapped: %w", &ch.Exception{Code: 497}), "CH_PERMISSION", 497},
		{&ch.Exception{Code: 516}, "CH_AUTHENTICATION", 516},
		{&ch.Exception{Code: 9999}, "CH_UNKNOWN", 9999},
		{x509.UnknownAuthorityError{}, "CH_TLS", 0},
		{adapterError("CH_VERSION_BELOW_FLOOR", "old server", sqlconnect.ErrNotSupported), "CH_VERSION_BELOW_FLOOR", 0},
		{errors.New("Code: 497"), "CH_UNKNOWN", 0},
	} {
		info := db.ClassifyError(tt.err)
		require.Equal(t, tt.code, info.Code)
		require.Equal(t, tt.server, info.ServerCode)
	}
	_, err := db.Begin()
	require.ErrorIs(t, err, sqlconnect.ErrNotSupported)
	_, err = db.Prepare("SELECT 1")
	require.ErrorIs(t, err, sqlconnect.ErrNotSupported)
}
