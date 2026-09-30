package chtest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScrub(t *testing.T) {
	msg := "Code: 62. Syntax error near 'pw_Sentinel_1' ... BY 'pw_Sentinel_1'"
	require.Equal(t, "Code: 62. Syntax error near '[redacted]' ... BY '[redacted]'", scrub(msg, "pw_Sentinel_1", ""))
}
