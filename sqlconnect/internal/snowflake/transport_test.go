package snowflake

import (
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGuardedTransportIsOpaque(t *testing.T) {
	transport := newGuardedTransport() // the same construction used by NewDB
	t.Cleanup(transport.CloseIdleConnections)

	var roundTripper http.RoundTripper = transport
	_, isHTTPTransport := roundTripper.(*http.Transport)
	require.False(t, isHTTPTransport, "gosnowflake must not mutate the shared TLS config")
	require.Equal(t, uint16(tls.VersionTLS12), transport.next.TLSClientConfig.MinVersion)
	require.NotNil(t, transport.next.DialContext, "the egress-guarded dialer must be preserved")
}
