package clickhouse

import "testing"

func TestVersionFloor(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		version          string
		valid, supported bool
	}{
		{"24.8.1", true, false}, {"25.7.99.1", true, false}, {"25.8.0", true, true},
		{"25.8.1.3", true, true}, {"26.3.33.24", true, true}, {"25.10.0", true, true},
		{"25.8", false, false}, {"garbage", false, false}, {"25.-8.1", false, false}, {"25.8.0junk", false, false},
	} {
		t.Run(tt.version, func(t *testing.T) {
			supported, err := supportedVersion(tt.version)
			if (err == nil) != tt.valid || supported != tt.supported {
				t.Fatalf("got (%v, %v), want valid=%v supported=%v", supported, err, tt.valid, tt.supported)
			}
		})
	}
}
