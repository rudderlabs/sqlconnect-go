package clickhousequery_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
)

func TestAccountKeys(t *testing.T) {
	expected := []string{"host", "port", "database", "user", "password", "secure", "skipVerify"}
	keys := clickhousequery.AccountKeys()
	require.Equal(t, expected, keys)
	for index := range keys {
		keys[index] = "changed"
	}
	require.Equal(t, expected, clickhousequery.AccountKeys())
}

func TestExcludedKeys(t *testing.T) {
	expected := []string{
		"protocol", "nativePort", "caCertificate", "tunnel_info", "sshHost", "cluster", "settings",
		"timeout", "allowLoopback", "allowPlainHTTP", "skipHostValidation",
	}
	keys := clickhousequery.ExcludedKeys()
	require.Equal(t, expected, keys)
	for index := range keys {
		keys[index] = "changed"
	}
	require.Equal(t, expected, clickhousequery.ExcludedKeys())
}
