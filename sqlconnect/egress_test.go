package sqlconnect_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

func TestSetEgressPolicy(t *testing.T) {
	t.Cleanup(func() { require.NoError(t, sqlconnect.SetEgressPolicy(nil)) })

	require.NoError(t, sqlconnect.SetEgressPolicy([]string{"172.20.0.0/16", "10.100.0.0/16"}))
	require.ErrorContains(t, util.ValidateHost("172.20.0.1"), "blocked range")
	require.ErrorContains(t, util.ValidateHost("10.100.0.1"), "blocked range")

	err := sqlconnect.SetEgressPolicy([]string{"192.168.0.0/16", " 198.51.100.0/24"})
	require.ErrorContains(t, err, `invalid CIDR " 198.51.100.0/24"`)
	require.ErrorContains(t, util.ValidateHost("172.20.0.1"), "blocked range")
	require.ErrorContains(t, util.ValidateHost("10.100.0.1"), "blocked range")
	require.NoError(t, util.ValidateHost("192.168.0.1"))

	require.NoError(t, sqlconnect.SetEgressPolicy([]string{"192.168.0.0/16"}))
	require.NoError(t, util.ValidateHost("172.20.0.1"))
	require.NoError(t, util.ValidateHost("10.100.0.1"))
	require.ErrorContains(t, util.ValidateHost("192.168.0.1"), "blocked range")

	require.NoError(t, sqlconnect.SetEgressPolicy([]string{}))
	require.NoError(t, util.ValidateHost("192.168.0.1"))

	require.NoError(t, sqlconnect.SetEgressPolicy([]string{"192.168.0.0/16"}))
	require.NoError(t, sqlconnect.SetEgressPolicy(nil))
	require.NoError(t, util.ValidateHost("192.168.0.1"))
}
