package clickhousequery

import (
	"errors"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

// Details are the bounded fields of a ClickHouse adapter error.
type Details struct {
	Code, Category, Field string
	// Detail is the adapter message that Error prints after the code. It
	// never holds server text or a credential, so a caller can show it as is.
	Detail     string
	ServerCode int32
	RetryAfter time.Duration
}

// Describe returns the details of the first adapter error in err's chain.
func Describe(err error) (Details, bool) {
	var e *cherr.Error
	if !errors.As(err, &e) {
		return Details{}, false
	}
	return Details{
		Code: e.Code, Category: cherr.Category(e.Code), Field: e.Field, Detail: e.Detail,
		ServerCode: e.ServerCode, RetryAfter: e.RetryAfter,
	}, true
}
