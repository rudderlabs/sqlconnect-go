package bigquery

import ("testing"; "github.com/stretchr/testify/require")

type mockCT struct{ n string }
func (m mockCT) DatabaseTypeName() string { return m.n }
func (mockCT) DecimalSize() (int64, int64, bool) { return 0, 0, false }

func TestColumnTypeMapper_Array(t *testing.T) {
	for _, raw := range []string{"ARRAY<STRING>", "array<string>", "ARRAY<BYTES>"} {
		require.Equal(t, "array", columnTypeMapper(mockCT{raw}), raw)
	}
	require.Equal(t, "json", columnTypeMapper(mockCT{"ARRAY<INT64>"}), "non-string array stays json")
	require.Equal(t, "string", columnTypeMapper(mockCT{"STRING"}))
	require.Equal(t, "json", columnTypeMapper(mockCT{"STRUCT<a INT64>"}))
}
