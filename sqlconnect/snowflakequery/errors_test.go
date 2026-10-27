package snowflakequery_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/snowflakequery"
)

func TestDescribe(t *testing.T) {
	sfErr := &gosnowflake.SnowflakeError{SQLState: "42601", Number: 2043, Message: "do not expose"}
	var typedNil *gosnowflake.SnowflakeError
	for _, tc := range []struct {
		name string
		err  error
		want snowflakequery.Details
		ok   bool
	}{
		{"direct", sfErr, snowflakequery.Details{SQLState: "42601", Number: 2043}, true},
		{"wrapped", fmt.Errorf("operation: %w", sfErr), snowflakequery.Details{SQLState: "42601", Number: 2043}, true},
		{"typed nil", typedNil, snowflakequery.Details{}, false},
		{"wrapped typed nil", fmt.Errorf("operation: %w", typedNil), snowflakequery.Details{}, false},
		{"not Snowflake", errors.New("other"), snowflakequery.Details{}, false},
		{"nil", nil, snowflakequery.Details{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			details, ok := snowflakequery.Describe(tc.err)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, details)
		})
	}
}
