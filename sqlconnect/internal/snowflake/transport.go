package snowflake

import (
	"crypto/tls"
	"net/http"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

// opaqueTransport prevents gosnowflake/v2 from type-asserting our pool-shared
// *http.Transport and writing TLSClientConfig on every connection open. The
// guarded dialer and TLS floor are configured before the transport is used.
type opaqueTransport struct{ next *http.Transport }

func newGuardedTransport() *opaqueTransport {
	transport := util.GuardedHTTPTransport(false)
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	return &opaqueTransport{next: transport}
}

func (t *opaqueTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return t.next.RoundTrip(r)
}

// CloseIdleConnections lets http.Client.CloseIdleConnections reach the wrapped transport.
func (t *opaqueTransport) CloseIdleConnections() { t.next.CloseIdleConnections() }
