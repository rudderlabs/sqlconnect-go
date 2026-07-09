package bigquery

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type mockCT struct{ n string }

func (m mockCT) DatabaseTypeName() string        { return m.n }
func (mockCT) DecimalSize() (int64, int64, bool) { return 0, 0, false }

func TestColumnTypeMapper_Array(t *testing.T) {
	for _, raw := range []string{"ARRAY<STRING>", "array<string>", "ARRAY<BYTES>"} {
		require.Equal(t, "array", columnTypeMapper(mockCT{raw}), raw)
	}
	require.Equal(t, "unsupported", columnTypeMapper(mockCT{"ARRAY<INT64>"}), "non-string array is unsupported")
	require.Equal(t, "string", columnTypeMapper(mockCT{"STRING"}))
	require.Equal(t, "unsupported", columnTypeMapper(mockCT{"STRUCT<a INT64>"}))
	require.Equal(t, "unsupported", columnTypeMapper(mockCT{"RECORD"}))
}
