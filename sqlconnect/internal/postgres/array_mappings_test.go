package postgres

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
		{"text[]", "array"},
		{"TEXT[]", "array"},
		{"character varying[]", "array"},
		{"character varying(255)[]", "array"},
		{"varchar(10)[]", "array"},
		{"_text", "array"}, // lib/pq internal name for text[]
		{"_varchar", "array"},
		// non-string scalar arrays → json
		{"integer[]", "json"},
		{"int[]", "json"},
		{"bigint[]", "json"},
		{"boolean[]", "json"},
		{"_int4", "json"},
		{"_bool", "json"},
		{"smallint[]", "json"},
		{"real[]", "json"},
		{"numeric(10,2)[]", "json"},
		{"text[][]", "json"}, // nested — element is not a string scalar
		// information_schema bare ARRAY → json (no element type)
		{"ARRAY", "json"},
		// non-arrays unaffected
		{"text", "string"},
		{"integer", "int"},
		{"jsonb", "json"},
		{"numeric(10,2)", "float"},
	}
	for _, c := range cases {
		if got := columnTypeMapper(fakeColumnType{c.raw}); got != c.want {
			t.Errorf("columnTypeMapper(%q) = %q, want %q", c.raw, got, c.want)
		}
	}
}
