package driver

import "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"

// result is the result of an Exec. MaxCompute reports no affected-row count
// that the driver reads, so RowsAffected returns an error.
type result struct{}

func (result) LastInsertId() (int64, error) {
	return 0, mcerr.Local(mcerr.CategoryRefused, "LastInsertId is not supported")
}

func (result) RowsAffected() (int64, error) {
	return 0, mcerr.Local(mcerr.CategoryRefused, "the affected row count is unknown")
}
