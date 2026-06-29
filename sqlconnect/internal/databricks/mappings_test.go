package databricks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// mockColumnType implements base.ColumnType for testing the column type mapper.
type mockColumnType struct {
	databaseTypeName string
}

func (m mockColumnType) DatabaseTypeName() string { return m.databaseTypeName }
func (m mockColumnType) DecimalSize() (precision, scale int64, ok bool) {
	return 0, 0, false
}

func TestColumnTypeMapper_Array(t *testing.T) {
	t.Run("string-element arrays map to array rudder-type", func(t *testing.T) {
		for _, raw := range []string{"ARRAY<STRING>", "array<string>", "ARRAY<VARCHAR(255)>", "ARRAY<CHAR(3)>"} {
			require.Equal(t, "array", columnTypeMapper(mockColumnType{raw}), raw)
		}
	})

	t.Run("non-string-element arrays stay json (unsupported in v1)", func(t *testing.T) {
		for _, raw := range []string{"ARRAY<BIGINT>", "ARRAY<INT>", "ARRAY<DOUBLE>", "ARRAY<STRUCT<a:INT>>"} {
			require.Equal(t, "json", columnTypeMapper(mockColumnType{raw}), raw)
		}
	})

	t.Run("scalar types unaffected", func(t *testing.T) {
		require.Equal(t, "string", columnTypeMapper(mockColumnType{"STRING"}))
		require.Equal(t, "int", columnTypeMapper(mockColumnType{"BIGINT"}))
		require.Equal(t, "json", columnTypeMapper(mockColumnType{"MAP<STRING,INT>"}))
	})
}

// The data-graph schema path (useStandardTypeMappings) uses the legacy mapper, so it must
// agree with columnTypeMapper on string-element arrays (DESCRIBE TABLE yields the full type).
func TestLegacyColumnTypeMapper_Array(t *testing.T) {
	for _, raw := range []string{"ARRAY<STRING>", "array<string>", "ARRAY<VARCHAR(255)>"} {
		require.Equal(t, "array", legacyColumnTypeMapper(mockColumnType{raw}), raw)
	}
	for _, raw := range []string{"ARRAY<BIGINT>", "ARRAY<INT>"} {
		require.Equal(t, "json", legacyColumnTypeMapper(mockColumnType{raw}), raw)
	}
	require.Equal(t, "string", legacyColumnTypeMapper(mockColumnType{"STRING"}))
}
