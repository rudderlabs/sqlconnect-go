package integrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	sshx "github.com/gliderlabs/ssh"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
	"golang.org/x/crypto/ssh"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	tunnelhelper "github.com/rudderlabs/sql-tunnels/tunnel/testhelper"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
)

func TestSshTunnelScenarios(t *testing.T, warehouse string, configJSON json.RawMessage) {
	sshPort, err := kithelper.GetFreePort()
	require.NoError(t, err, "it should be able to get a free port")
	server, privateKey := newSshServer(t, sshPort)
	t.Cleanup(func() {
		err := server.Close()
		require.NoError(t, err, "it should be able to close the ssh server")
	})
	tunnelConfig := sshtunnel.Config{
		User:       "root",
		Host:       "127.0.0.1",
		Port:       strconv.Itoa(sshPort),
		PrivateKey: string(privateKey),
	}

	configJSON, err = sjson.SetBytes(configJSON, "tunnel_info", tunnelConfig)
	require.NoError(t, err, "it should be able to set the tunnel info in the config")

	t.Run("ssh tunnel", func(t *testing.T) {
		db, err := sqlconnect.NewDB(warehouse, configJSON)
		require.NoError(t, err, "it should be able to create a new DB")
		defer func() { _ = db.Close() }()
		err = db.Ping()
		require.NoError(t, err, "it should be able to ping the db")
		_, err = db.ListSchemas(context.Background())
		require.NoError(t, err, "it should be able to list schemas")
		require.GreaterOrEqual(t, server.connections, 1, "ssh server should have received at least 1 connection")
	})
}

type testsshserver struct {
	*sshx.Server

	connections int
}

func (s *testsshserver) DirectTCPIPHandler(srv *sshx.Server, conn *ssh.ServerConn, newChan ssh.NewChannel, ctx sshx.Context) {
	s.connections++
	sshx.DirectTCPIPHandler(srv, conn, newChan, ctx)
}

func newSshServer(t *testing.T, port int) (server *testsshserver, privateKey []byte) {
	t.Helper()
	server = &testsshserver{}
	var publicKey []byte
	privateKey, publicKey = tunnelhelper.SSHKeyPairs(t)
	pkey, _, _, _, err := sshx.ParseAuthorizedKey(publicKey)
	require.NoError(t, err)

	server.Server = &sshx.Server{
		LocalPortForwardingCallback: sshx.LocalPortForwardingCallback(func(ctx sshx.Context, dhost string, dport uint32) bool {
			return true
		}),
		Addr: fmt.Sprintf("0.0.0.0:%d", port),
		Handler: sshx.Handler(func(s sshx.Session) {
			_, _ = io.WriteString(s, "Remote forwarding available...\n")
			select {}
		}),
		ReversePortForwardingCallback: sshx.ReversePortForwardingCallback(func(ctx sshx.Context, host string, port uint32) bool {
			return true
		}),
		PublicKeyHandler: func(ctx sshx.Context, key sshx.PublicKey) bool {
			return sshx.KeysEqual(key, pkey)
		},
		ChannelHandlers: map[string]sshx.ChannelHandler{
			"direct-tcpip": server.DirectTCPIPHandler,
			"session":      sshx.DefaultSessionHandler,
		},
	}
	go func() {
		err := server.ListenAndServe()
		require.Equal(t, sshx.ErrServerClosed, err)
	}()

	require.Eventually(t, func() bool {
		con, err := net.Dial("tcp", server.Addr)
		if err != nil {
			return false
		}
		_ = con.Close()
		return true
	}, 1*time.Second, 10*time.Millisecond)

	return server, privateKey
}
