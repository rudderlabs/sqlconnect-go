package chtest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdminFailureHidesExceptionText(t *testing.T) {
	body := "Code: 62. DB::Exception: Syntax error: failed at position 40 ('pw_It\\'s_Sentinel') " +
		"near 'customer_value_Sentinel': CREATE USER x BY 'pw_It\\'s_Sentinel'. (SYNTAX_ERROR) (version 26.3.33.24 (official build))\n"
	got := adminFailure(400, body)
	require.Equal(t, "status 400, code 62, SYNTAX_ERROR", got)
	require.NotContains(t, got, "Sentinel")
	require.Equal(t, "status 502", adminFailure(502, "bad gateway"))
}
