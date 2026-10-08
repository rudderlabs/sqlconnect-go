package sqlconnect_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

func TestSetEgressPolicy(t *testing.T) {
	require.NoError(t, sqlconnect.SetEgressPolicy([]string{"172.20.0.0/16", "10.100.0.0/16"}))
	t.Cleanup(func() { require.NoError(t, sqlconnect.SetEgressPolicy(nil)) })
	require.ErrorContains(t, util.ValidateHost("172.20.0.1"), "blocked range")
	require.ErrorContains(t, util.ValidateHost("10.100.0.1"), "blocked range")

	err := sqlconnect.SetEgressPolicy([]string{"192.168.0.0/16", "nonsense"})
	require.ErrorContains(t, err, `invalid CIDR "nonsense"`)
	require.ErrorContains(t, util.ValidateHost("172.20.0.1"), "blocked range")
	require.ErrorContains(t, util.ValidateHost("10.100.0.1"), "blocked range")
	require.NoError(t, util.ValidateHost("192.168.0.1"))
}
