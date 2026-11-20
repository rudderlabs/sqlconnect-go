package driver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"reflect"
	"time"
	"unsafe"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// The Instance Tunnel's HTTP client has no deadline: SDK v0.4.26 gives a
// tunnel a zero HttpTimeout, and the SDK builds the http.Client itself, with
// no option to pass one in. A whole-request timeout would cut a long result
// stream, so the connector gives the tunnel's rest client its own http.Client
// whose connections have an idle deadline instead: every read and write on
// the connection must make progress within idle. A session create or a range
// open whose server never answers, and a result stream that goes silent,
// then fail with a timeout (a transport error, so a range is resumed) and the
// connection closes, ending the request instead of leaving it open until the
// server or TCP gives up. A slow but steady stream is never cut.

// idleHTTPClient returns an http.Client whose connections time out a read or
// a write that makes no progress for idle. Its other settings follow the
// SDK's own tunnel client: proxy from the environment, HTTP/1.1, no
// transparent compression (the tunnel's deflate is decoded by the SDK).
func idleHTTPClient(idle, connectTimeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
	return &http.Client{Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &idleConn{Conn: conn, idle: idle}, nil
		},
		ForceAttemptHTTP2:  false,
		DisableCompression: true,
		// A pooled connection is dropped well before its pending read times
		// out, so a request rarely picks one that is about to close.
		IdleConnTimeout: idle / 2,
	}}
}

// idleConn sets a fresh deadline before every read and write, so an
// operation fails only when it waits idle without progress. The deadline
// runs only while a read or write waits: a caller that pauses between reads
// does not use it up. A write also extends the read deadline, because an
// answer is due after it: the transport's read on a pooled connection is
// already waiting, and an extended deadline applies to a pending read.
type idleConn struct {
	net.Conn
	idle time.Duration
}

func (c *idleConn) Read(b []byte) (int, error) {
	if err := c.SetReadDeadline(time.Now().Add(c.idle)); err != nil {
		return 0, err
	}
	return c.Conn.Read(b)
}

func (c *idleConn) Write(b []byte) (int, error) {
	if err := c.SetDeadline(time.Now().Add(c.idle)); err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}

// setHTTPClient makes rc send its requests through hc. RestClient keeps its
// http.Client in the unexported field _client and builds one on first use;
// setting it before any request is the only way to supply our own. The field
// is checked by name and type, so an SDK that renames or retypes it fails
// here, at the tunnel's first use, and in TestSetHTTPClient.
func setHTTPClient(rc *restclient.RestClient, hc *http.Client) error {
	f := reflect.ValueOf(rc).Elem().FieldByName("_client")
	if !f.IsValid() || f.Type() != reflect.TypeFor[*http.Client]() {
		return errors.New("the ODPS SDK's RestClient has no _client *http.Client field")
	}
	reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Set(reflect.ValueOf(hc))
	return nil
}
