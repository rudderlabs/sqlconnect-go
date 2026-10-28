package snowflake

import (
	"errors"
	"fmt"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"
	"github.com/stretchr/testify/require"
)

func TestObjectInaccessibleError(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		accessible bool
	}{
		{"missing object", &gosnowflake.SnowflakeError{Number: sfErrObjectNotFound}, false},
		{"insufficient privileges", fmt.Errorf("list tables: %w", &gosnowflake.SnowflakeError{Number: sfErrInsufficientPrivileges}), false},
		{"other Snowflake error", &gosnowflake.SnowflakeError{Number: 1234, SQLState: "42601"}, true},
		{"SQLState alone is not inaccessible", &gosnowflake.SnowflakeError{SQLState: "42601"}, true},
		{"other error", errors.New("other"), true},
		{"nil", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, !tc.accessible, isObjectInaccessibleError(tc.err))
		})
	}
}
