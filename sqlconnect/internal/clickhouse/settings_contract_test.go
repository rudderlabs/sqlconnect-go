package clickhouse

import (
	"reflect"
	"testing"
)

// This oracle intentionally does not use overflowModes or driverMaxResultBytes.
func p12ScratchWant() map[string]any {
	return map[string]any{
		"join_use_nulls": 1, "session_timezone": "UTC", "select_sequential_consistency": 1,
		"transform_null_in": 0, "data_type_default_nullable": 0, "enable_parallel_replicas": 0,
		"limit": 0, "offset": 0, "additional_result_filter": "",
		"async_insert": 0, "wait_for_async_insert": 1, "send_progress_in_http_headers": 0,
		"insert_null_as_default": 0,
		"timeout_overflow_mode":  "throw", "timeout_overflow_mode_leaf": "throw",
		"read_overflow_mode": "throw", "read_overflow_mode_leaf": "throw",
		"group_by_overflow_mode": "throw", "sort_overflow_mode": "throw", "result_overflow_mode": "throw",
		"set_overflow_mode": "throw", "join_overflow_mode": "throw", "transfer_overflow_mode": "throw",
		"distinct_overflow_mode": "throw",
	}
}

func p12ReadWant() map[string]any {
	m := p12ScratchWant()
	delete(m, "async_insert")
	delete(m, "wait_for_async_insert")
	m["readonly"] = 2
	m["cancel_http_readonly_queries_on_client_close"] = 1
	m["max_result_bytes"] = 268435456
	return m
}

func p12UnionWant(seconds int) map[string]any {
	m := p12ReadWant()
	m["async_insert"], m["wait_for_async_insert"] = 0, 1
	m["final"], m["skip_unavailable_shards"] = 1, 0
	m["max_replica_delay_for_distributed_queries"] = 1
	m["fallback_to_stale_replicas_for_distributed_queries"] = 0
	m["max_execution_time"] = seconds
	return m
}

func TestDriverSettingsLiteralContract(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func() map[string]any
		want  map[string]any
	}{
		{"scratch", driverScratchSettings, p12ScratchWant()},
		{"read", driverReadSettings, p12ReadWant()},
		{"union", func() map[string]any { return stage2UnionSettings(37) }, p12UnionWant(37)},
		{"control", controlSettings, map[string]any{"send_progress_in_http_headers": 0}},
		{"hello", helloSettings, map[string]any{"send_progress_in_http_headers": 0, "limit": 0, "offset": 0, "additional_result_filter": ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.build()
			if !reflect.DeepEqual(tc.want, got) {
				t.Fatalf("settings = %#v; want %#v", got, tc.want)
			}
			for k := range got {
				got[k] = "hostile profile"
			}
			got["unexpected"] = 1
			if fresh := tc.build(); !reflect.DeepEqual(tc.want, fresh) {
				t.Fatalf("a previous caller changed the next statement: %#v", fresh)
			}
		})
	}
}

func FuzzUnionBudget(f *testing.F) {
	for _, seconds := range []int{1, 2, 37, 120, 3600} {
		f.Add(seconds)
	}
	f.Fuzz(func(t *testing.T, seconds int) {
		if seconds < 1 || seconds > 86400 {
			t.Skip()
		}
		if got := stage2UnionSettings(seconds); !reflect.DeepEqual(p12UnionWant(seconds), got) {
			t.Fatalf("budget %d: %#v", seconds, got)
		}
	})
}
