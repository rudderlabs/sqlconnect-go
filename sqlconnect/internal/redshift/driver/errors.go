package driver

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

var (
	ErrNotSupported = errors.New("not supported")
	ErrDSNEmpty     = errors.New("dsn is empty")
	ErrConnClosed   = errors.New("connection closed")
	ErrBeforeCommit = errors.New("transaction is not committed")
	ErrNotInTx      = errors.New("not in transaction")
	ErrInTx         = errors.New("already in transaction")
)

// StatementError is a statement the Data API describes as failed or aborted.
// The API relays the server message and carries no SQLSTATE.
type StatementError struct {
	Status  types.StatusString
	Message string
}

func (e *StatementError) Error() string {
	if e.Status == types.StatusStringAborted {
		return "query aborted: " + e.Message
	}
	return "query failed: " + e.Message
}
