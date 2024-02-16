package util

import (
	"fmt"
	"net"
)

// ValidateHost checks if the hostname is resolvable and that it doesn't correspond to localhost.
func ValidateHost(hostname string) error {
	addrs, err := net.LookupHost(hostname)
	if err != nil {
		return fmt.Errorf("error looking up hostname %s: %v", hostname, err)
	}

	for _, addr := range addrs {
		if addr == "127.0.0.1" || addr == "0.0.0.0" {
			return fmt.Errorf("invalid host name in credentials")
		}
	}
	return nil
}
