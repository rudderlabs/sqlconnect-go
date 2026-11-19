package driver

import (
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
)

// result is the result of an Exec. A job reports no affected-row count in any
// response the driver fetches (a count needs an extra instance-summary call per
// job), so RowsAffected and LastInsertId return an error that wraps
// sqlconnect.ErrNotSupported, never a count of 0 a caller could trust.
type result struct{}

func (result) LastInsertId() (int64, error) {
	return 0, mcerr.LocalWrap(mcerr.CategoryRefused, "LastInsertId is not supported", sqlconnect.ErrNotSupported)
}

func (result) RowsAffected() (int64, error) {
	return 0, mcerr.LocalWrap(mcerr.CategoryRefused, "the affected row count is unknown", sqlconnect.ErrNotSupported)
}
