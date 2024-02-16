package sqlconnect_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestNewDB(t *testing.T) {
	_, err := sqlconnect.NewDB("invalid", []byte{})
	require.Error(t, err, "should return error for invalid db name")
}
