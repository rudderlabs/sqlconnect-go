package clickhouse

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
)

// This contract deliberately does not derive its keys from overflowModes.
// Removing a safety setting must not also remove the assertion for that key.
func TestSettingsLiteralContract(t *testing.T) {
	want := map[string]any{
		"join_use_nulls": 1, "session_timezone": "UTC", "select_sequential_consistency": 1,
		"transform_null_in": 0, "data_type_default_nullable": 0, "enable_parallel_replicas": 0,
		"limit": 0, "offset": 0, "additional_result_filter": "",
		"async_insert": 0, "wait_for_async_insert": 1, "send_progress_in_http_headers": 0,
		"insert_null_as_default": 0,
		"timeout_overflow_mode":  "throw", "timeout_overflow_mode_leaf": "throw",
		"read_overflow_mode": "throw", "read_overflow_mode_leaf": "throw",
		"group_by_overflow_mode": "throw", "sort_overflow_mode": "throw",
		"result_overflow_mode": "throw", "set_overflow_mode": "throw",
		"join_overflow_mode": "throw", "transfer_overflow_mode": "throw",
		"distinct_overflow_mode": "throw",
	}
	require.Equal(t, want, driverScratchSettings(), "copy writes must fail on every overflow")
	wantRead := maps.Clone(want)
	delete(wantRead, "async_insert")
	delete(wantRead, "wait_for_async_insert")
	wantRead["readonly"] = 2
	wantRead["cancel_http_readonly_queries_on_client_close"] = 1
	wantRead["max_result_bytes"] = 256 << 20
	require.Equal(t, wantRead, driverReadSettings())

	// A caller mutating one returned map cannot weaken subsequent statements.
	changed := driverScratchSettings()
	changed["read_overflow_mode"] = "break"
	delete(changed, "timeout_overflow_mode")
	require.Equal(t, want, driverScratchSettings())
	require.Equal(t, wantRead, driverReadSettings())
}
