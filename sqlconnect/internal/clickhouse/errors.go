// Package clickhouse is the ClickHouse driver of sqlconnect. It talks HTTPS to
// ClickHouse through the rudderlabs clickhouse-go fork.
package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"syscall"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

// serverCodes maps a ClickHouse server code to its registry code, from the
// driver LLD "Error classification" table. A code outside it is CH_UNKNOWN.
var serverCodes = map[int32]string{}

func init() {
	for code, list := range map[string][]int32{
		cherr.CodeAuthentication: {516, 192, 193, 194},
		cherr.CodePermission:     {497, 291, 164, 195, 452},
		cherr.CodeQueryInvalid:   {62, 43, 46, 47, 53, 70, 115},
		cherr.CodeTimeout:        {159, 160, 209},
		cherr.CodeResource:       {173, 191, 201, 202, 241, 243, 252, 158, 290, 307, 396},
		cherr.CodeNetwork:        {95, 96, 198, 210, 279},
		cherr.CodeObjectNotFound: {60, 81},
		cherr.CodeCancelled:      {394},
		cherr.CodeUnavailable:    {242},
	} {
		for _, sc := range list {
			serverCodes[sc] = code
		}
	}
}

// fixedMessages holds the one message that bound returns for each code. The
// server text never reaches a caller, because a hostile endpoint can echo a
// credential in it.
var fixedMessages = map[string]string{
	cherr.CodeConfigInvalid:           "the account configuration is invalid",
	cherr.CodeCAInvalid:               "the CA bundle is invalid",
	cherr.CodeWorkspaceNotEnabled:     "the workspace is not enabled for ClickHouse",
	cherr.CodeHostNotAllowed:          "the host resolves to an address that is not allowed",
	cherr.CodeDNSFailed:               "the host name did not resolve",
	cherr.CodeRedirectRefused:         "the server sent a redirect, which is refused",
	cherr.CodeSortingKeyUnavailable:   "the table has no usable sorting key",
	cherr.CodeTLS:                     "the TLS handshake or certificate check failed",
	cherr.CodeRateLimited:             "the server limited the request rate",
	cherr.CodeClusterUnsupported:      "the cluster setup is not supported",
	cherr.CodeVersionBelowFloor:       "the server version is below the supported floor",
	cherr.CodeCatalogUnsupported:      "catalogs are not supported",
	cherr.CodeCrossDatabaseMove:       "moving a table to another database is not supported",
	cherr.CodeTransactionsUnsupported: "transactions are not supported",
	cherr.CodePrepareUnsupported:      "prepared statements are not supported",
	cherr.CodeTypeUnsupported:         "the column type is not supported",
	cherr.CodeInvalidReference:        "the table reference is invalid",
	cherr.CodeInvalidIdentifier:       "the identifier is invalid",
	cherr.CodeQueryInvalid:            "the server refused the query as invalid",
	cherr.CodeObjectNotFound:          "the table or database does not exist",
	cherr.CodeValueEncoding:           "a value could not be encoded",
	cherr.CodeDuplicateColumn:         "a column name appears more than once",
	cherr.CodeCountOverflow:           "a row count overflowed",
	cherr.CodeDDLNotVisible:           "the schema change did not become visible in time",
	cherr.CodeRowCountMismatch:        "the row count does not match",
	cherr.CodeBaselineMissing:         "the previous snapshot does not exist",
	cherr.CodeSchemaMismatch:          "the schema does not match",
	cherr.CodeLostResponse:            "the response was lost after the statement was sent",
	cherr.CodeOutcomeUnknown:          "the statement outcome is unknown",
	cherr.CodeScratchCleanupFailed:    "the scratch cleanup failed",
	cherr.CodeAuthentication:          "the server refused the credentials",
	cherr.CodePermission:              "the user lacks a required privilege or setting",
	cherr.CodeTimeout:                 "the request timed out",
	cherr.CodeResource:                "the server ran out of a resource or hit a limit",
	cherr.CodeNetwork:                 "the network request failed",
	cherr.CodeCancelled:               "the request was cancelled",
	cherr.CodeUnavailable:             "the server is unavailable",
	cherr.CodeUnknown:                 "the request failed",
}

func info(code string, server int32) sqlconnect.ErrorInfo {
	return sqlconnect.ErrorInfo{Category: cherr.Category(code), Code: code, ServerCode: server}
}

// classify applies the order of the driver LLD "Error classification" section.
// It matches types only, never message text.
func classify(err error) sqlconnect.ErrorInfo {
	if err == nil {
		return sqlconnect.ErrorInfo{}
	}
	switch {
	case errors.Is(err, context.Canceled):
		return info(cherr.CodeCancelled, 0)
	case errors.Is(err, context.DeadlineExceeded):
		return info(cherr.CodeTimeout, 0)
	}
	var own *cherr.Error
	if errors.As(err, &own) {
		return info(own.Code, own.ServerCode)
	}
	var ex *ch.Exception
	if errors.As(err, &ex) {
		if code, ok := serverCodes[ex.Code]; ok {
			return info(code, ex.Code)
		}
		return info(cherr.CodeUnknown, ex.Code)
	}
	var he *ch.HTTPError
	if errors.As(err, &he) {
		switch he.StatusCode {
		case 502, 503, 504:
			return info(cherr.CodeUnavailable, 0)
		default:
			return info(cherr.CodeNetwork, 0)
		}
	}
	if isTLSError(err) {
		return info(cherr.CodeTLS, 0)
	}
	var ne net.Error
	if errors.As(err, &ne) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, driver.ErrBadConn) {
		return info(cherr.CodeNetwork, 0)
	}
	return info(cherr.CodeUnknown, 0)
}

func isTLSError(err error) bool {
	var (
		cv  *tls.CertificateVerificationError
		ua  x509.UnknownAuthorityError
		hn  x509.HostnameError
		ci  x509.CertificateInvalidError
		rh  tls.RecordHeaderError
		alt tls.AlertError
	)
	return errors.As(err, &cv) || errors.As(err, &ua) || errors.As(err, &hn) ||
		errors.As(err, &ci) || errors.As(err, &rh) || errors.As(err, &alt)
}

// bound turns err into an adapter error with the fixed message of its code,
// the server code and a redacted cause. An adapter error in the chain returns
// unchanged.
func bound(op, field string, err error) error {
	if err == nil {
		return nil
	}
	var own *cherr.Error
	if errors.As(err, &own) {
		return own
	}
	i := classify(err)
	e := wrap(i.Code, field, op+": "+fixedMessages[i.Code], err)
	e.ServerCode = i.ServerCode
	return e
}

// wrap is the only way driver code attaches a cause to an adapter error.
func wrap(code, field, detail string, cause error) *cherr.Error {
	return cherr.Wrap(code, field, detail, redact(cause))
}

// redact keeps only the parts of err that callers test with errors.Is or
// errors.As and that carry no server or transport text: the context errors,
// ErrNotSupported, an adapter error, and a copy of a server exception without
// its text fields. It returns nil when nothing is kept.
//
// The exception copy keeps only Code. The fork fills Name and CodeName from the
// response text, so a hostile endpoint controls them as much as Message.
func redact(err error) error {
	if err == nil {
		return nil
	}
	var parts []error
	for _, keep := range []error{context.Canceled, context.DeadlineExceeded, sqlconnect.ErrNotSupported} {
		if errors.Is(err, keep) {
			parts = append(parts, keep)
		}
	}
	var own *cherr.Error
	if errors.As(err, &own) {
		parts = append(parts, own)
	}
	var ex *ch.Exception
	var he *ch.HTTPError
	switch {
	case errors.As(err, &ex):
		clean := &ch.Exception{Code: ex.Code}
		if errors.As(err, &he) {
			parts = append(parts, &ch.HTTPError{StatusCode: he.StatusCode, Err: clean})
		} else {
			parts = append(parts, clean)
		}
	case errors.As(err, &he):
		parts = append(parts, &ch.HTTPError{StatusCode: he.StatusCode, Err: errors.New("response body redacted")})
	}
	return errors.Join(parts...)
}
