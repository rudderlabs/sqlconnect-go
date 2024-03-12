package snowflake

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUndefinedInArray(t *testing.T) {
	r, err := undefinedInArray.Replace("[\n  1,\n  2,\n  3,\n  undefined\n]", "${1}null", 0, -1)
	require.NoError(t, err)
	require.Equal(t, "[\n  1,\n  2,\n  3,\n  null\n]", r)

	r, err = undefinedInArray.Replace("[\n  undefined,\n  1,\n  2,\n  3\n]", "${1}null", 0, -1)
	require.NoError(t, err)
	require.Equal(t, "[\n  null,\n  1,\n  2,\n  3\n]", r)

	r, err = undefinedInArray.Replace("[\n  1,\n  undefined,\n  2,\n  3\n]", "${1}null", 0, -1)
	require.NoError(t, err)
	require.Equal(t, "[\n  1,\n  null,\n  2,\n  3\n]", r)

	r, err = undefinedInArray.Replace("[\n  undefined,\n  undefined,\n  undefined\n]", "${1}null", 0, -1)
	require.NoError(t, err)
	require.Equal(t, "[\n  null,\n  null,\n  null\n]", r)

	r, err = undefinedInArray.Replace("[\n  \"undefined string\",\n  2,\n  3,\n  undefined\n]", "${1}null", 0, -1)
	require.NoError(t, err)
	require.Equal(t, "[\n  \"undefined string\",\n  2,\n  3,\n  null\n]", r)
}
