package maxcomputequery_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/maxcomputequery"
)

func TestStopInfo(t *testing.T) {
	e := mcerr.Context(context.Canceled, "inst1", "terminated")
	e.Stop = string(sqlconnect.StopLanded)
	id, outcome, ok := maxcomputequery.StopInfo(e)
	require.True(t, ok)
	require.Equal(t, "inst1", id)
	require.Equal(t, sqlconnect.StopLanded, outcome)

	_, _, ok = maxcomputequery.StopInfo(mcerr.FromJob("ODPS-0130131: Table not found"))
	require.False(t, ok, "not a stop")
	_, _, ok = maxcomputequery.StopInfo(errors.New("x"))
	require.False(t, ok)
}
