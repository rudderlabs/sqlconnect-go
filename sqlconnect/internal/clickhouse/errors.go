package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func (db *DB) ClassifyError(err error) sqlconnect.ErrorInfo { return classifyError(err) }

func classifyError(err error) sqlconnect.ErrorInfo {
	info := sqlconnect.ErrorInfo{Category: "unknown", Code: "CH_UNKNOWN"}
	if err == nil {
		return sqlconnect.ErrorInfo{}
	}
	if errors.Is(err, context.Canceled) {
		info.Category, info.Code = "cancellation", "CH_CANCELLED"
		return info
	}
	if errors.Is(err, context.DeadlineExceeded) {
		info.Category, info.Code = "timeout", "CH_TIMEOUT"
		return info
	}
	var own *driverError
	if errors.As(err, &own) {
		info.Code = own.code
		switch own.code {
		case "CH_CONFIG_INVALID", "CH_SORTING_KEY_UNAVAILABLE":
			info.Category = "configuration"
		case "CH_INVALID_REFERENCE", "CH_INVALID_IDENTIFIER", "CH_INVALID_QUERY":
			info.Category = "syntax"
		case "CH_RELATION_NOT_FOUND":
			info.Category = "not_found"
		case "CH_VALUE_ENCODING", "CH_DUPLICATE_COLUMN", "CH_COUNT_OVERFLOW":
			info.Category = "data"
		default:
			info.Category = "unsupported"
		}
		return info
	}
	var exception *ch.Exception
	if errors.As(err, &exception) {
		info.ServerCode = exception.Code
		switch exception.Code {
		case 516, 192, 193, 194:
			info.Category, info.Code = "authentication", "CH_AUTHENTICATION"
		case 497, 291, 164, 195:
			info.Category, info.Code = "permission", "CH_PERMISSION"
		case 62, 43, 46, 47, 53, 70, 115:
			info.Category, info.Code = "syntax", "CH_QUERY_INVALID"
		case 159, 160, 209:
			info.Category, info.Code = "timeout", "CH_TIMEOUT"
		case 173, 201, 202, 241, 243, 252, 158, 290, 307, 396:
			info.Category, info.Code = "resource", "CH_RESOURCE"
		case 95, 96, 198, 210, 279:
			info.Category, info.Code = "network", "CH_NETWORK"
		case 60, 81:
			info.Category, info.Code = "not_found", "CH_OBJECT_NOT_FOUND"
		case 394:
			info.Category, info.Code = "cancellation", "CH_CANCELLED"
		case 242:
			info.Category, info.Code = "availability", "CH_UNAVAILABLE"
		}
		return info
	}
	var cert *tls.CertificateVerificationError
	var unknownCA x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalidCert x509.CertificateInvalidError
	var record tls.RecordHeaderError
	if errors.As(err, &cert) || errors.As(err, &unknownCA) || errors.As(err, &hostname) || errors.As(err, &invalidCert) || errors.As(err, &record) {
		info.Category, info.Code = "network", "CH_TLS"
		return info
	}
	var network net.Error
	if errors.As(err, &network) {
		info.Category, info.Code = "network", "CH_NETWORK"
		if network.Timeout() {
			info.Category, info.Code = "timeout", "CH_TIMEOUT"
		}
	}
	return info
}
