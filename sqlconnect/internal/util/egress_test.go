package util

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockedReason(t *testing.T) {
	for _, tc := range []struct {
		name          string
		ip            string
		allowLoopback bool
		want          string
	}{
		{name: "public", ip: "8.8.8.8"},
		{name: "loopback blocked", ip: "127.0.0.1", want: "loopback"},
		{name: "loopback allowed", ip: "127.0.0.1", allowLoopback: true},
		{name: "metadata remains blocked", ip: "169.254.169.254", allowLoopback: true, want: "link-local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, blockedReason(net.ParseIP(tc.ip), tc.allowLoopback))
		})
	}

	_, cidr, err := net.ParseCIDR("172.20.0.0/16")
	require.NoError(t, err)
	SetBlockedCIDRs([]*net.IPNet{cidr})
	t.Cleanup(func() { SetBlockedCIDRs(nil) })
	require.Equal(t, "blocked range", blockedReason(net.ParseIP("172.20.0.1"), false))
	require.Empty(t, blockedReason(net.ParseIP("8.8.8.8"), false))
}

func TestGuardedDialContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		addr string
	}{
		{name: "literal loopback", addr: "127.0.0.1:9"},
		{name: "resolved hostname", addr: "localhost:9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := GuardedDialContext(false)(context.Background(), "tcp", tc.addr)
			require.Nil(t, conn)
			var blocked *BlockedHostError
			require.ErrorAs(t, err, &blocked)
			require.Equal(t, "loopback", blocked.Reason)
			require.True(t, blocked.IP.IsLoopback())
		})
	}

	t.Run("allowed loopback can attempt connection", func(t *testing.T) {
		conn, err := GuardedDialContext(true)(context.Background(), "tcp", "127.0.0.1:9")
		if conn != nil {
			require.NoError(t, conn.Close())
		}
		var blocked *BlockedHostError
		require.False(t, errors.As(err, &blocked))
	})

	t.Run("operator CIDR", func(t *testing.T) {
		_, cidr, err := net.ParseCIDR("172.20.0.0/16")
		require.NoError(t, err)
		SetBlockedCIDRs([]*net.IPNet{cidr})
		t.Cleanup(func() { SetBlockedCIDRs(nil) })

		conn, err := GuardedDialContext(false)(context.Background(), "tcp", "172.20.0.1:9")
		require.Nil(t, conn)
		var blocked *BlockedHostError
		require.ErrorAs(t, err, &blocked)
		require.Equal(t, "blocked range", blocked.Reason)
		require.Equal(t, "connection to 172.20.0.1 blocked: blocked range", blocked.Error())
	})
}
