package mcerr

import (
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

var odpsCodePattern = regexp.MustCompile(`(?i)\bODPS-(\d{7})\b`)

// classification is what classify reads off a raw error before redaction.
type classification struct {
	category  string
	code      string
	retryable bool
}

// classifyText classifies the text of a failed job or of an HTTP error
// message by its ODPS code (LLD section 12, from findings 10 and 15).
func classifyText(text string) classification {
	m := odpsCodePattern.FindStringSubmatch(text)
	code := ""
	if m != nil {
		code = "ODPS-" + m[1]
	}
	c := classification{code: code}
	switch {
	case strings.Contains(text, "ODPS-0121145"):
		// A runtime overflow is reported inside an 0130071 plan failure.
		c.category, c.code = CategoryData, "ODPS-0121145"
	case strings.HasPrefix(code, "ODPS-0121"):
		// Other runtime data errors, such as 0121011 for an invalid regular
		// expression built from row values: a rerun fails the same way.
		c.category = CategoryData
	case code == "ODPS-0130131":
		c.category = CategoryNotFound
	case code == "ODPS-0130013", code == "ODPS-0420095":
		c.category = CategoryPermission
	case code == "ODPS-0130071" && strings.Contains(text, "full scan with all partitions"):
		c.category = CategoryFullScan
	case code == "ODPS-0130071" && strings.Contains(text, "ORDER BY must be used with a LIMIT"):
		c.category = CategoryInternal // the orderby hint is missing: a driver bug
	case code == "ODPS-0130071", code == "ODPS-0130141", code == "ODPS-0130241", code == "ODPS-0130252", code == "ODPS-0110061":
		c.category = CategorySemantic
	case code == "ODPS-0130161":
		c.category = CategorySyntax
	case code == "ODPS-0130211":
		c.category = CategoryAlreadyExists
	case code == "ODPS-0424171":
		c.category = CategoryCancelled
	case code == "ODPS-0420061":
		c.category = CategoryInternal // an MCQA quota: the driver never asks for MCQA
	case code == "ODPS-0410051", code == "ODPS-0410042":
		c.category = CategoryAuth
	case code == "ODPS-0420111" || strings.Contains(text, "0420111"):
		c.code = "ODPS-0420111"
		if strings.Contains(text, "Project not found") {
			c.category = CategoryProjectNotFound
		} else {
			c.category = CategoryNotFound // "Database not found": a schema
		}
	case strings.Contains(text, "odps.sql.submit.mode") && strings.Contains(text, "script"):
		c.category = CategoryRefused // several statements without script mode
	case code != "":
		c.category, c.retryable = CategoryUnknown, true // bounded by the caller
	default:
		c.category = CategoryUnknown
	}
	return c
}

// classifyHTTP classifies an SDK REST error by its status, its <Code> and
// the ODPS code in its message.
func classifyHTTP(e restclient.HttpError) classification {
	msg := e.ErrorMessage
	if msg == nil {
		msg = restclient.NewErrorMessage(e.Body)
	}
	errCode, text := "", ""
	if msg != nil {
		errCode, text = msg.ErrorCode, msg.Message
	}
	body := string(e.Body)
	switch {
	case e.StatusCode == http.StatusNotFound && (strings.Contains(body, "odps-frontend-server") || strings.Contains(strings.ToLower(body), "<html")):
		return classification{category: CategoryBadEndpoint, code: "HTTP-404"}
	case errCode == "SignatureNotMatch", errCode == "AccessKeyIdNotFound", errCode == "InvalidAccessKeyId", errCode == "SignatureDoesNotMatch",
		errCode == "InvalidParameter" && strings.Contains(text, "ODPS-0410051"):
		return classification{category: CategoryAuth, code: errCode}
	case errCode == "NoPermission", strings.Contains(text, "ODPS-0420095"):
		return classification{category: CategoryPermission, code: errCode}
	case errCode == "ParseError":
		return classification{category: CategorySyntax, code: firstNonEmpty(odpsCode(text), errCode)}
	case e.StatusCode == http.StatusTooManyRequests || e.StatusCode == http.StatusServiceUnavailable:
		return classification{category: CategoryUnknown, code: "HTTP-" + strconv.Itoa(e.StatusCode), retryable: true}
	}
	c := classifyText(text)
	if c.code == "" {
		c.code = firstNonEmpty(errCode, "HTTP-"+strconv.Itoa(e.StatusCode))
	}
	if c.category == CategoryUnknown && e.StatusCode >= 500 {
		c.retryable = true
	}
	return c
}

func odpsCode(text string) string {
	if m := odpsCodePattern.FindStringSubmatch(text); m != nil {
		return "ODPS-" + m[1]
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func asHTTPError(err error) (restclient.HttpError, bool) {
	if e, ok := errors.AsType[restclient.HttpError](err); ok {
		return e, true
	}
	if e, ok := errors.AsType[*restclient.HttpError](err); ok && e != nil {
		return *e, true
	}
	return restclient.HttpError{}, false
}

// ServerCode returns the numeric part of an ODPS code, such as 130131.
func ServerCode(code string) int32 {
	if m := odpsCodePattern.FindStringSubmatch(code); m != nil {
		n, _ := strconv.ParseInt(m[1], 10, 32)
		return int32(n)
	}
	return 0
}
