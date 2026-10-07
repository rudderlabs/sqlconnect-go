package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCleanupConfigFromEnv(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		_, ok := cleanupConfigFromEnv(cleanupConfig{Env: "MISSING_TEST_CREDENTIALS"})
		require.False(t, ok)
	})

	t.Run("empty", func(t *testing.T) {
		t.Setenv("EMPTY_TEST_CREDENTIALS", "  ")
		_, ok := cleanupConfigFromEnv(cleanupConfig{Env: "EMPTY_TEST_CREDENTIALS"})
		require.False(t, ok)
	})

	t.Run("configured", func(t *testing.T) {
		t.Setenv("CONFIGURED_TEST_CREDENTIALS", ` {"database":"warehouse"} `)
		value, ok := cleanupConfigFromEnv(cleanupConfig{
			Env: "CONFIGURED_TEST_CREDENTIALS",
			Fn:  func(value string) string { return value + " transformed" },
		})
		require.True(t, ok)
		require.Equal(t, `{"database":"warehouse"} transformed`, value)
	})
}
