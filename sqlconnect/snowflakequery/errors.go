package snowflakequery

import (
	"errors"

	"github.com/snowflakedb/gosnowflake/v2"
)

// Details are the bounded fields of a Snowflake driver error.
type Details struct {
	SQLState string
	Number   int
}

// Describe returns the details of the first Snowflake driver error in err's chain.
func Describe(err error) (Details, bool) {
	var sfErr *gosnowflake.SnowflakeError
	if !errors.As(err, &sfErr) || sfErr == nil {
		return Details{}, false
	}
	return Details{SQLState: sfErr.SQLState, Number: sfErr.Number}, true
}
