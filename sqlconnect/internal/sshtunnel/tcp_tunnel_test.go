package sshtunnel_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper"
	tunnelhelper "github.com/rudderlabs/sql-tunnels/tunnel/testhelper"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
)

func TestTcpTunnelErrors(t *testing.T) {
	const remoteHost = "remote_host"
	const remotePort = 1234
	c := sshtunnel.Config{
		User:       "user",
		Host:       "host",
		Port:       "22",
		PrivateKey: "private_key",
	}

	t.Run("invalid config", func(t *testing.T) {
		_, err := sshtunnel.NewTcpTunnel(sshtunnel.Config{}, remoteHost, remotePort)
		require.Error(t, err, "it should return an error when config is invalid")
		require.ErrorContains(t, err, "invalid ssh tunnel configuration")
	})

	t.Run("invalid private key", func(t *testing.T) {
		c := c
		_, err := sshtunnel.NewTcpTunnel(c, remoteHost, remotePort)
		require.Error(t, err, "it should return an error when private key is invalid")
		require.ErrorContains(t, err, "parsing private key")
	})

	t.Run("invalid endoint", func(t *testing.T) {
		privateKey, _ := tunnelhelper.SSHKeyPairs(t)
		port, err := testhelper.GetFreePort()
		require.NoError(t, err, "it should not return an error")
		c := c
		c.PrivateKey = string(privateKey)
		c.Port = strconv.Itoa(port)
		_, err = sshtunnel.NewTcpTunnel(c, remoteHost, remotePort)
		require.Error(t, err, "it should return an error when endpoint is invalid")
		require.ErrorContains(t, err, "dial error")
	})
}
