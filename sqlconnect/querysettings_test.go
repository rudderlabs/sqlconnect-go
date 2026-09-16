package sqlconnect

import (
	"context"
	"testing"
)

func TestQuerySettingsCopies(t *testing.T) {
	t.Parallel()
	if QuerySettings(context.Background()) != nil {
		t.Fatal("absent settings must leave native contexts intact")
	}
	original := map[string]any{"final": 1}
	ctx := WithQuerySettings(context.Background(), original)
	original["final"] = 0
	first := QuerySettings(ctx)
	if first["final"] != 1 {
		t.Fatal("caller mutation changed attached settings")
	}
	first["final"] = 0
	if QuerySettings(ctx)["final"] != 1 {
		t.Fatal("reader mutation changed attached settings")
	}
	if len(QuerySettings(WithQuerySettings(ctx, map[string]any{"max_threads": 2}))) != 1 {
		t.Fatal("child settings must replace parent settings")
	}
}
