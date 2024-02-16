package util_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

func TestValidateHost(t *testing.T) {
	t.Run("valid host", func(t *testing.T) {
		err := util.ValidateHost("github.com")
		require.NoError(t, err)
	})

	t.Run("invalid host", func(t *testing.T) {
		err := util.ValidateHost("!@#$.$%^")
		require.Error(t, err)
	})

	t.Run("localhost", func(t *testing.T) {
		err := util.ValidateHost("localhost")
		require.Error(t, err)
	})
}
