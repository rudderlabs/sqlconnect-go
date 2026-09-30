package sqlconnect_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestValidationStageError_Contract(t *testing.T) {
	inner := errors.New("CH_PERMISSION (INSERT): the user lacks a required privilege")
	err := fmt.Errorf("validate: %w", sqlconnect.ValidationStageError{Stage: 3, Tag: "grant_check", Err: inner})
	var se sqlconnect.ValidationStageError
	require.True(t, errors.As(err, &se))
	require.Equal(t, sqlconnect.ValidationStageError{Stage: 3, Tag: "grant_check", Err: inner}, se)
	require.ErrorIs(t, err, inner)
	require.Equal(t, "validation stage 3 (grant_check): "+inner.Error(), se.Error())
	_, ok := sqlconnect.ValidationOptionsFrom(context.Background())
	require.False(t, ok)
	o, ok := sqlconnect.ValidationOptionsFrom(sqlconnect.WithValidationOptions(context.Background(), sqlconnect.ValidationOptions{SyncLogPruning: true}))
	require.True(t, ok && o.SyncLogPruning)
}
