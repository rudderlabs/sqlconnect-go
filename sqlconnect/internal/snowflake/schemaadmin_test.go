package snowflake

import (
	"errors"
	"fmt"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestObjectInaccessibleError(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		accessible bool
		want       sqlconnect.ErrorInfo
	}{
		{"missing object", &gosnowflake.SnowflakeError{Number: sfErrObjectNotFound}, false, sqlconnect.ErrorInfo{Category: "not_found", Code: "2043", ServerCode: 2043}},
		{"insufficient privileges", fmt.Errorf("list tables: %w", &gosnowflake.SnowflakeError{Number: sfErrInsufficientPrivileges}), false, sqlconnect.ErrorInfo{Category: "permission", Code: "2003", ServerCode: 2003}},
		{"other Snowflake error", &gosnowflake.SnowflakeError{Number: 1234, SQLState: "42601"}, true, sqlconnect.ErrorInfo{Code: "1234", ServerCode: 1234}},
		{"SQLState alone is not already exists", &gosnowflake.SnowflakeError{SQLState: "42601"}, true, sqlconnect.ErrorInfo{Code: "0"}},
		{"other error", errors.New("other"), true, sqlconnect.ErrorInfo{}},
		{"nil", nil, true, sqlconnect.ErrorInfo{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, !tc.accessible, isObjectInaccessibleError(tc.err))
			var classifier sqlconnect.ErrorClassifier = &DB{}
			require.Equal(t, tc.want, classifier.ClassifyError(tc.err))
		})
	}
}
