package sshtunnel_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
)

func TestValidate(t *testing.T) {
	c := sshtunnel.Config{
		User:       "user",
		Host:       "host",
		Port:       "22",
		PrivateKey: "private_key",
	}

	t.Run("valid", func(t *testing.T) {
		c := c
		err := c.Validate()
		require.NoError(t, err, "it should not return an error")
	})

	t.Run("empty user", func(t *testing.T) {
		c := c
		c.User = ""
		err := c.Validate()
		require.Error(t, err, "it should return an error when user is empty")
	})

	t.Run("empty host", func(t *testing.T) {
		c := c
		c.Host = ""
		err := c.Validate()
		require.Error(t, err, "it should return an error when host is empty")
	})

	t.Run("empty port", func(t *testing.T) {
		c := c
		c.Port = ""
		err := c.Validate()
		require.Error(t, err, "it should return an error when port is empty")
	})

	t.Run("invalid port", func(t *testing.T) {
		c := c
		c.Port = "invalid"
		err := c.Validate()
		require.Error(t, err, "it should return an error when port is invalid")
	})

	t.Run("empty private key", func(t *testing.T) {
		c := c
		c.PrivateKey = ""
		err := c.Validate()
		require.Error(t, err, "it should return an error when private key is empty")
	})
}

func TestParseInlineConfig(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		config := `{
			"useSSH": true,
			"sshUser": "user",
			"sshHost": "host",
			"sshPort": "22",
			"sshPrivateKey" : "private_key"
		}`
		c, err := sshtunnel.ParseInlineConfig([]byte(config))
		require.NoError(t, err, "it should not return an error")
		require.NotNil(t, c, "it should return a non-nil config")
		require.Equal(t, "user", c.User, "it should return the correct user")
		require.Equal(t, "host", c.Host, "it should return the correct host")
		require.Equal(t, "22", c.Port, "it should return the correct port")
		require.Equal(t, "private_key", c.PrivateKey, "it should return the correct private key")
	})

	t.Run("useSSH false", func(t *testing.T) {
		config := `{
			"useSSH": false
		}`
		c, err := sshtunnel.ParseInlineConfig([]byte(config))
		require.NoError(t, err, "it should not return an error")
		require.Nil(t, c, "it should return a nil config")
	})

	t.Run("invalid", func(t *testing.T) {
		config := `{
			"useSSH": true,
			"sshUser": "user",
			"sshHost": "host",
			"sshPort": 22
		}`
		_, err := sshtunnel.ParseInlineConfig([]byte(config))
		require.Error(t, err, "it should return an error")
	})
}
