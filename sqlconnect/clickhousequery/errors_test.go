package clickhousequery_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

func TestDescribe_ExposesAdapterFields(t *testing.T) {
	e := cherr.New(cherr.CodeRateLimited, "", "the server asked the client to slow down")
	e.RetryAfter = 7 * time.Second
	d, ok := clickhousequery.Describe(fmt.Errorf("op: %w", e))
	require.True(t, ok)
	require.Equal(t, clickhousequery.Details{Code: "CH_RATE_LIMITED", Category: "transient", RetryAfter: 7 * time.Second}, d)
	_, ok = clickhousequery.Describe(errors.New("plain"))
	require.False(t, ok)
	require.Regexp(t, `^retl-[0-9a-f-]{36}$`, clickhousequery.NewQueryID())
	require.Equal(t, 2*time.Hour, clickhousequery.MaxRunBudget)
}
