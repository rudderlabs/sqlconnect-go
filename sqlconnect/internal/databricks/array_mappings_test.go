package databricks

import "testing"

type fakeColumnType struct{ name string }

func (f fakeColumnType) DatabaseTypeName() string          { return f.name }
func (f fakeColumnType) DecimalSize() (int64, int64, bool) { return 0, 0, false }

func TestColumnTypeMapper_Array(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		// string-element arrays → array (the only filterable arrays in v1)
		{"ARRAY<STRING>", "array"},
		{"array<string>", "array"},
		{"ARRAY < STRING >", "array"}, // whitespace tolerated
		// non-string scalar arrays → json (numeric/boolean membership can't match string filters)
		{"ARRAY<INT>", "json"},
		{"ARRAY<BIGINT>", "json"},
		{"ARRAY<DOUBLE>", "json"},
		{"ARRAY<BOOLEAN>", "json"},
		{"ARRAY<TIMESTAMP>", "json"},
		// struct / nested / map element arrays → json (Feature 4)
		{"ARRAY<STRUCT<a: INT>>", "json"},
		{"ARRAY<ARRAY<STRING>>", "json"},
		{"ARRAY<MAP<STRING, INT>>", "json"},
		// bare ARRAY (no element type) → unchanged (conservative json)
		{"ARRAY", "json"},
		// non-arrays unaffected
		{"STRING", "string"},
		{"INT", "int"},
		{"STRUCT<a: INT>", "json"},
		{"MAP<STRING, INT>", "json"},
		{"DECIMAL(10,2)", "float"},
	}
	for _, c := range cases {
		if got := columnTypeMapper(fakeColumnType{c.raw}); got != c.want {
			t.Errorf("columnTypeMapper(%q) = %q, want %q", c.raw, got, c.want)
		}
	}
}
