package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	"github.com/stretchr/testify/require"
)

// Embeds the interface so only the calls a statement makes need bodies.
type fakeClient struct {
	RedshiftClient
	describe *redshiftdata.DescribeStatementOutput
}

func (f fakeClient) ExecuteStatement(context.Context, *redshiftdata.ExecuteStatementInput, ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error) {
	return &redshiftdata.ExecuteStatementOutput{Id: aws.String("statement")}, nil
}

func (f fakeClient) BatchExecuteStatement(context.Context, *redshiftdata.BatchExecuteStatementInput, ...func(*redshiftdata.Options)) (*redshiftdata.BatchExecuteStatementOutput, error) {
	return &redshiftdata.BatchExecuteStatementOutput{Id: aws.String("batch")}, nil
}

func (f fakeClient) DescribeStatement(context.Context, *redshiftdata.DescribeStatementInput, ...func(*redshiftdata.Options)) (*redshiftdata.DescribeStatementOutput, error) {
	return f.describe, nil
}

func TestStatementFailureIsTyped(t *testing.T) {
	const serverMessage = `ERROR: relation "s.t" does not exist`
	failed := &redshiftdata.DescribeStatementOutput{Status: types.StatusStringFailed, Error: aws.String(serverMessage)}
	aborted := &redshiftdata.DescribeStatementOutput{Status: types.StatusStringAborted, Error: aws.String("Query cancelled by user")}

	t.Run("query", func(t *testing.T) {
		conn := newConnection(fakeClient{describe: failed}, &RedshiftConfig{})
		_, err := conn.QueryContext(t.Context(), "SELECT 1 FROM s.t", nil)
		stmtErr, ok := errors.AsType[*StatementError](err)
		require.True(t, ok, "%v", err)
		require.Equal(t, types.StatusStringFailed, stmtErr.Status)
		require.Equal(t, serverMessage, stmtErr.Message)
		require.EqualError(t, err, "query failed: "+serverMessage)
	})

	t.Run("exec", func(t *testing.T) {
		conn := newConnection(fakeClient{describe: failed}, &RedshiftConfig{})
		_, err := conn.ExecContext(t.Context(), "DROP TABLE s.t", nil)
		stmtErr, ok := errors.AsType[*StatementError](err)
		require.True(t, ok, "%v", err)
		require.Equal(t, serverMessage, stmtErr.Message)
	})

	t.Run("aborted", func(t *testing.T) {
		conn := newConnection(fakeClient{describe: aborted}, &RedshiftConfig{})
		_, err := conn.QueryContext(t.Context(), "SELECT 1", nil)
		stmtErr, ok := errors.AsType[*StatementError](err)
		require.True(t, ok, "%v", err)
		require.Equal(t, types.StatusStringAborted, stmtErr.Status)
		require.EqualError(t, err, "query aborted: Query cancelled by user")
	})

	t.Run("batch commit", func(t *testing.T) {
		conn := newConnection(fakeClient{describe: failed}, &RedshiftConfig{})
		tx, err := conn.BeginTx(t.Context(), driver.TxOptions{})
		require.NoError(t, err)
		_, err = conn.ExecContext(t.Context(), "INSERT INTO s.t VALUES (1)", nil)
		require.NoError(t, err)
		_, err = conn.ExecContext(t.Context(), "INSERT INTO s.t VALUES (2)", nil)
		require.NoError(t, err)
		err = tx.Commit()
		stmtErr, ok := errors.AsType[*StatementError](err)
		require.True(t, ok, "%v", err)
		require.Equal(t, serverMessage, stmtErr.Message)
	})

	t.Run("missing error text", func(t *testing.T) {
		conn := newConnection(fakeClient{describe: &redshiftdata.DescribeStatementOutput{Status: types.StatusStringFailed}}, &RedshiftConfig{})
		_, err := conn.QueryContext(t.Context(), "SELECT 1", nil)
		require.EqualError(t, err, "query failed: ")
	})
}
