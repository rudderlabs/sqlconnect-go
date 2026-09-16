package clickhouse

import (
	"fmt"
	"strconv"
	"strings"
)

func supportedVersion(version string) (bool, error) {
	parts := strings.Split(version, ".")
	if len(parts) < 3 || len(parts) > 4 {
		return false, fmt.Errorf("invalid ClickHouse version")
	}
	parsed := make([]uint64, len(parts))
	for i, part := range parts {
		n, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return false, fmt.Errorf("invalid ClickHouse version")
		}
		parsed[i] = n
	}
	floor := []uint64{25, 8, 0}
	for i, n := range floor {
		if parsed[i] != n {
			return parsed[i] > n, nil
		}
	}
	return true, nil
}
