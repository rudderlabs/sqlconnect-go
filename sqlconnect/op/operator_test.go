package op

import "testing"

func TestIsValid(t *testing.T) {
	valid := []Operator{
		Eq, Neq, In, Nin, Gt, Gte, Lt, Lte, Like, NLike, Btw, Nbtw, Inlast, Null, Nnull,
		// array column filter operators (Feature 5)
		ContainsAny, ContainsAll, NContains, Empty, Nempty, SizeEq, SizeGt, SizeLt,
	}
	for _, o := range valid {
		if !IsValid(o) {
			t.Errorf("IsValid(%q) = false, want true", o)
		}
	}

	invalid := []Operator{"", "contains", "aempty", "size", "unknown", "ARRAY_CONTAINS"}
	for _, o := range invalid {
		if IsValid(o) {
			t.Errorf("IsValid(%q) = true, want false", o)
		}
	}
}

func TestArrayOperatorWireValues(t *testing.T) {
	want := map[Operator]string{
		ContainsAny: "contains_any",
		ContainsAll: "contains_all",
		NContains:   "ncontains",
		Empty:       "empty",
		Nempty:      "nempty",
		SizeEq:      "size_eq",
		SizeGt:      "size_gt",
		SizeLt:      "size_lt",
	}
	for o, s := range want {
		if string(o) != s {
			t.Errorf("operator wire value = %q, want %q", string(o), s)
		}
	}
}
