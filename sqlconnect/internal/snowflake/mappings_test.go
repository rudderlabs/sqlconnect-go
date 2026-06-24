package snowflake

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

func TestColumnTypeMapper(t *testing.T) {
	t.Run("ARRAY maps to array rudder-type", func(t *testing.T) {
		result := columnTypeMapper(mockColumnType{"ARRAY"})
		require.Equal(t, "array", result, "Snowflake ARRAY column must map to rudder-type 'array'")
	})

	t.Run("VARIANT maps to json rudder-type", func(t *testing.T) {
		result := columnTypeMapper(mockColumnType{"VARIANT"})
		require.Equal(t, "json", result, "Snowflake VARIANT column must map to rudder-type 'json'")
	})

	t.Run("OBJECT maps to json rudder-type", func(t *testing.T) {
		result := columnTypeMapper(mockColumnType{"OBJECT"})
		require.Equal(t, "json", result, "Snowflake OBJECT column must map to rudder-type 'json'")
	})
}

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
