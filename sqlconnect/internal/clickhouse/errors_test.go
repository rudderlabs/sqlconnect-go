package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

func TestScratchCleanup_WorkingDatabaseMessage(t *testing.T) {
	require.Equal(t, "the RudderStack working database cleanup failed", fixedMessages[cherr.CodeScratchCleanupFailed])
}

func TestSQ22_ServerCodes(t *testing.T) {
	cases := map[string][]int32{
		"CH_AUTHENTICATION":   {516, 192, 193, 194},
		"CH_PERMISSION":       {497, 291, 164, 195, 452},
		"CH_QUERY_INVALID":    {62, 43, 46, 47, 53, 70, 115},
		"CH_TIMEOUT":          {159, 160, 209},
		"CH_RESOURCE":         {173, 191, 201, 202, 241, 243, 252, 158, 290, 307, 396},
		"CH_NETWORK":          {95, 96, 198, 210, 279},
		"CH_OBJECT_NOT_FOUND": {60, 81},
		"CH_CANCELLED":        {394},
		"CH_UNAVAILABLE":      {242},
		"CH_UNKNOWN":          {1, 999},
	}
	for code, servers := range cases {
		for _, sc := range servers {
			ex := &ch.Exception{Code: sc, Message: "raw sentinel-pw"}
			for _, wrapped := range []error{ex, &ch.HTTPError{StatusCode: 500, Err: ex}, fmt.Errorf("q: %w", ex)} {
				info := classify(wrapped)
				require.Equal(t, code, info.Code, "server code %d", sc)
				require.Equal(t, sc, info.ServerCode)
				require.Equal(t, cherr.Category(code), info.Category)
			}
		}
	}
	require.Equal(t, sqlconnect.ErrorInfo{}, classify(nil))
}

func TestSQ22_OrderAndTransportErrors(t *testing.T) {
	body := errors.New("response body: x")
	for want, errs := range map[string][]error{
		"CH_CANCELLED":        {context.Canceled, &ch.HTTPError{StatusCode: 500, Err: fmt.Errorf("%w %w", context.Canceled, &ch.Exception{Code: 516})}},
		"CH_TIMEOUT":          {fmt.Errorf("x: %w", context.DeadlineExceeded)},
		"CH_HOST_NOT_ALLOWED": {cherr.New(cherr.CodeHostNotAllowed, "host", "x")},
		"CH_RATE_LIMITED":     {cherr.New(cherr.CodeRateLimited, "", "x")},
		"CH_UNAVAILABLE":      {&ch.HTTPError{StatusCode: 502, Err: body}, &ch.HTTPError{StatusCode: 503, Err: body}, &ch.HTTPError{StatusCode: 504, Err: body}},
		"CH_TLS": {
			&url.Error{Op: "Post", Err: x509.UnknownAuthorityError{}},
			x509.HostnameError{Host: "h"},
			&tls.CertificateVerificationError{Err: body},
			tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"},
			x509.CertificateInvalidError{Reason: x509.Expired},
			tls.AlertError(42),
		},
		"CH_NETWORK": {
			&ch.HTTPError{StatusCode: 400, Err: body}, &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded},
			&net.OpError{Op: "read", Err: syscall.ECONNRESET}, io.EOF, io.ErrUnexpectedEOF, syscall.EPIPE,
		},
		"CH_UNKNOWN": {errors.New("other")},
	} {
		for _, e := range errs {
			require.Equal(t, want, classify(e).Code, "%T", e)
		}
	}
}

func TestSQ3_BoundedMessages(t *testing.T) {
	raw := &ch.HTTPError{StatusCode: 401, Err: &ch.Exception{
		Code: 516, Name: "sentinel-pw-123_Exception", CodeName: "SENTINEL_PW_123",
		Message: "Authentication failed: password sentinel-pw-123 is incorrect", StackTrace: "sentinel-pw-123",
	}}
	err := bound("validate", "", raw)
	require.NotContains(t, err.Error(), "sentinel-pw-123")
	require.Contains(t, err.Error(), "CH_AUTHENTICATION")
	require.Contains(t, err.Error(), "[server code 516]")
	requireCode(t, err, "CH_AUTHENTICATION")
	require.Equal(t, "CH_OBJECT_NOT_FOUND (db.t): create table: the table or database does not exist [server code 60]",
		bound("create table", "db.t", &ch.Exception{Code: 60, Message: "sentinel-pw-123"}).Error())
	var ex *ch.Exception
	require.ErrorAs(t, err, &ex, "callers can still test the server code")
	require.EqualValues(t, 516, ex.Code)
	require.Empty(t, ex.Name, "the fork parses Name from the response text")
	require.Empty(t, ex.CodeName, "the fork parses CodeName from the response text")
	require.Empty(t, ex.Message)
	require.Empty(t, ex.StackTrace)
	var he *ch.HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, 401, he.StatusCode)
	requireChainClean(t, err, "sentinel-pw-123")
	requireChainClean(t, err, "SENTINEL_PW_123")

	own := cherr.New(cherr.CodeHostNotAllowed, "host", "x")
	require.Same(t, own, bound("validate", "", own))
	require.Same(t, own, bound("validate", "", fmt.Errorf("dial: %w", own)))
	require.NoError(t, bound("x", "", nil))
	// A context error beside an adapter error wins, as in classify. An adapter
	// error that already carries the context error keeps its own code.
	perm := cherr.New(cherr.CodePermission, "", "x")
	for ctxErr, want := range map[error]string{context.Canceled: cherr.CodeCancelled, context.DeadlineExceeded: cherr.CodeTimeout} {
		joined := errors.Join(ctxErr, perm)
		require.Equal(t, classify(joined).Code, want)
		var got *cherr.Error
		require.ErrorAs(t, bound("x", "", joined), &got)
		require.Equal(t, want, got.Code)
		require.ErrorIs(t, got, ctxErr)
		carried := cherr.Wrap(cherr.CodeScratchCleanupFailed, "", "x", ctxErr)
		require.Same(t, carried, bound("x", "", carried))
	}
	require.ErrorIs(t, bound("x", "", fmt.Errorf("q: %w", context.Canceled)), context.Canceled)
	require.ErrorIs(t, bound("x", "", fmt.Errorf("q: %w", sqlconnect.ErrNotSupported)), sqlconnect.ErrNotSupported)
	requireChainClean(t, bound("dial", "", &net.OpError{Op: "dial", Err: errors.New("dial tcp sentinel-pw-123@10.0.0.1")}), "sentinel-pw-123")
	requireChainClean(t, bound("q", "", &ch.HTTPError{StatusCode: 400, Err: errors.New("response body: sentinel-pw-123")}), "sentinel-pw-123")
	requireChainClean(t, bound("q", "", fmt.Errorf("sentinel-pw-123: %w", context.DeadlineExceeded)), "sentinel-pw-123")
	requireChainClean(t, bound("q", "", &ch.Exception{Code: 62, Message: "sentinel-pw-123", Nested: []ch.Exception{{Message: "sentinel-pw-123"}}}), "sentinel-pw-123")

	for _, code := range cherr.Codes() {
		require.NotEmpty(t, fixedMessages[code], code)
	}
}

func TestSQ3_WrapRedacts(t *testing.T) {
	err := wrap(cherr.CodeDNSFailed, "host", "the host name did not resolve", &net.DNSError{Err: "sentinel-pw-123", Name: "h"})
	require.Equal(t, cherr.CodeDNSFailed, err.Code)
	requireChainClean(t, err, "sentinel-pw-123")
	require.NoError(t, redact(nil))
	require.NoError(t, redact(errors.New("sentinel-pw-123")))
}

func requireChainClean(t *testing.T, err error, secret string) {
	t.Helper()
	var walk func(e error)
	walk = func(e error) {
		if e == nil {
			return
		}
		require.NotContains(t, fmt.Sprintf("%v %+v %#v", e, e, e), secret)
		switch u := e.(type) {
		case interface{ Unwrap() []error }:
			for _, x := range u.Unwrap() {
				walk(x)
			}
		case interface{ Unwrap() error }:
			walk(u.Unwrap())
		}
	}
	walk(err)
}

func TestNoRawCherrWrap(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") || f == "errors.go" {
			continue
		}
		src, err := os.ReadFile(f)
		require.NoError(t, err)
		require.NotContains(t, string(src), "cherr.Wrap(", "%s: attach causes through wrap(), which redacts", f)
	}
}
