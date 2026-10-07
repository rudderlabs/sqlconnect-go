package clickhousequery

import "slices"

var accountKeys = []string{"host", "port", "database", "user", "password", "secure", "skipVerify"}

var excludedKeys = []string{
	"protocol", "nativePort", "caCertificate", "tunnel_info", "sshHost", "cluster", "settings",
	"timeout", "allowLoopback", "allowPlainHTTP", "skipHostValidation",
}

// AccountKeys returns a fresh copy of the account fields the driver accepts.
func AccountKeys() []string {
	return slices.Clone(accountKeys)
}

// ExcludedKeys returns a fresh copy of options other drivers or older drafts
// accept that this driver refuses when set.
func ExcludedKeys() []string {
	return slices.Clone(excludedKeys)
}
