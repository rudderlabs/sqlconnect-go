package clickhouse

// ParseConfigForTest exposes the parser with the test-only plain HTTP switch.
// The switch never appears in an exported production signature.
var ParseConfigForTest = parseConfig
