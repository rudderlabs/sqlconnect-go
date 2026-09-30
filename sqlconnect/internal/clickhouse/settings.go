package clickhouse

// overflowModes are the settings that choose between an error and a silently
// partial result when a limit is hit. Every driver map sets them to "throw".
var overflowModes = []string{
	"timeout_overflow_mode", "timeout_overflow_mode_leaf", "read_overflow_mode",
	"read_overflow_mode_leaf", "group_by_overflow_mode", "sort_overflow_mode", "result_overflow_mode",
	"set_overflow_mode", "join_overflow_mode", "transfer_overflow_mode", "distinct_overflow_mode",
}

// driverScratchSettings is the map for a statement that can write, when the
// caller gave no map of its own. It pins every setting that can change a
// result, so a user or profile default on the server cannot.
func driverScratchSettings() map[string]any {
	m := map[string]any{
		"join_use_nulls": 1, "session_timezone": "UTC", "select_sequential_consistency": 1,
		"transform_null_in": 0, "data_type_default_nullable": 0, "enable_parallel_replicas": 0,
		"limit": 0, "offset": 0, "additional_result_filter": "",
		"async_insert": 0, "wait_for_async_insert": 1, "send_progress_in_http_headers": 0,
	}
	for _, k := range overflowModes {
		m[k] = "throw"
	}
	return m
}

// driverReadSettings is the map for a read. readonly=2 refuses writes and still
// lets the statement send its own settings.
func driverReadSettings() map[string]any {
	m := driverScratchSettings()
	delete(m, "async_insert")
	delete(m, "wait_for_async_insert")
	m["readonly"] = 2
	m["cancel_http_readonly_queries_on_client_close"] = 1
	return m
}

// controlSettings is the map for a control statement such as a ping.
func controlSettings() map[string]any { return map[string]any{"send_progress_in_http_headers": 0} }

// stage2UnionSettings is the map for the stage 2 union read that fills a
// scratch table under a statement budget.
func stage2UnionSettings(budgetSeconds int) map[string]any {
	m := driverReadSettings()
	m["async_insert"], m["wait_for_async_insert"] = 0, 1
	m["final"] = 1
	m["skip_unavailable_shards"] = 0
	m["max_replica_delay_for_distributed_queries"] = 1
	m["fallback_to_stale_replicas_for_distributed_queries"] = 0
	m["max_execution_time"] = budgetSeconds
	return m
}
