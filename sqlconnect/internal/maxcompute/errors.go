package maxcompute

import (
	"context"
	"errors"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
)

var _ sqlconnect.ErrorClassifier = (*DB)(nil)

// ClassifyError returns the category and the ODPS (or HTTP) code of an error
// the driver returned. maxcomputequery.Describe adds the retryable flag, the
// instance id and whether the outcome is unknown.
func (db *DB) ClassifyError(err error) sqlconnect.ErrorInfo {
	if err == nil {
		return sqlconnect.ErrorInfo{}
	}
	if e, ok := errors.AsType[*mcerr.Error](err); ok {
		return sqlconnect.ErrorInfo{Category: e.Category, Code: e.Code, ServerCode: mcerr.ServerCode(e.Code)}
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return sqlconnect.ErrorInfo{Category: mcerr.CategoryCancelled}
	case errors.Is(err, sqlconnect.ErrNotSupported):
		return sqlconnect.ErrorInfo{Category: mcerr.CategoryRefused}
	}
	return sqlconnect.ErrorInfo{Category: mcerr.CategoryUnknown}
}
