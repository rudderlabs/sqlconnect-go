package op

import "testing"

func TestIsValid(t *testing.T) {
	valid := []Operator{
		Eq, Neq, In, Nin, Gt, Gte, Lt, Lte, Like, NLike, Btw, Nbtw, Inlast, Null, Nnull,
		// array column filter operators (Feature 5 — tight v1 set)
		Contains, NContains,
	}
	for _, o := range valid {
		if !IsValid(o) {
			t.Errorf("IsValid(%q) = false, want true", o)
		}
	}

	// not in the v1 set (deferred until the end-to-end harness exists)
	invalid := []Operator{
		"", "aempty", "size", "unknown", "ARRAY_CONTAINS",
		"contains_any", "contains_all", "empty", "nempty", "size_eq", "size_gt", "size_lt",
	}
	for _, o := range invalid {
		if IsValid(o) {
			t.Errorf("IsValid(%q) = true, want false", o)
		}
	}
}

func TestArrayOperatorWireValues(t *testing.T) {
	want := map[Operator]string{
		Contains:  "contains",
		NContains: "ncontains",
	}
	for o, s := range want {
		if string(o) != s {
			t.Errorf("operator wire value = %q, want %q", string(o), s)
		}
	}
}
