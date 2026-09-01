package sqlconnect

import (
	"encoding/json"
	"math"
	"testing"
)

// A NaN or ±Inf cell must not fail the marshal step. QueryAsync aborts the whole result set on
// the first mapper error, so before this was handled one such value made the entire query
// unservable — not just the offending row.
func TestNullifyNonFiniteFloats(t *testing.T) {
	row := map[string]any{
		"nan":      math.NaN(),
		"posInf":   math.Inf(1),
		"negInf":   math.Inf(-1),
		"nan32":    float32(math.NaN()),
		"finite":   1.5,
		"zero":     0.0,
		"str":      "keep",
		"nilValue": nil,
		"int":      int64(7),
		"maxFloat": math.MaxFloat64,
	}

	nullifyNonFiniteFloats(row)

	for _, k := range []string{"nan", "posInf", "negInf", "nan32"} {
		if row[k] != nil {
			t.Fatalf("%s: expected nil, got %v", k, row[k])
		}
	}
	if row["finite"] != 1.5 || row["zero"] != 0.0 || row["str"] != "keep" || row["int"] != int64(7) {
		t.Fatalf("finite values were altered: %v", row)
	}
	if row["maxFloat"] != math.MaxFloat64 {
		t.Fatalf("maxFloat altered: %v", row["maxFloat"])
	}

	b, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("marshal after nullify: %v", err)
	}
	var back map[string]any
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v, ok := back["nan"]; !ok || v != nil {
		t.Fatalf("nan should round-trip as JSON null, got %v (present=%v)", v, ok)
	}
}

// Guards the claim in the comment: without the normalisation this marshal fails, so the test
// would pass vacuously if nullifyNonFiniteFloats silently became a no-op.
func TestNonFiniteFloatsBreakMarshalWithoutNormalisation(t *testing.T) {
	if _, err := json.Marshal(map[string]any{"nan": math.NaN()}); err == nil {
		t.Fatal("expected json.Marshal to reject NaN; the normalisation would be unnecessary")
	}
}
