// Package odpsclient holds the ODPS (MaxCompute) client code that the source
// driver shares with the rudder-server MaxCompute destination.
//
// The endpoint check, the account constructors, the credential guard and the
// SafeError redaction start as a copy of rudder-server
// warehouse/integrations/maxcompute/client/odps.go and odps_credentials.go
// (master a64bfd9f2). parity_test.go holds the destination's patterns as
// fixtures and fails when a copy drifts. Additions the source needs beyond the
// destination live in redact.go, so this file stays a near-verbatim copy.
package odpsclient

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/credentials-go/credentials"
)

var (
	odpsProjectName       = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,127}$`)
	odpsRegionName        = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	odpsErrorCodePattern  = regexp.MustCompile(`(?i)ODPS-[0-9]{7}`)
	odpsNamedCodePattern  = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._-]{0,63}$`)
	odpsURLPattern        = regexp.MustCompile(`(?i)\b(?:https?|oss)://[^\s<>"']+`)
	odpsCredentialPattern = regexp.MustCompile(`(?i)\b(?:access[_-]?key(?:[_-]?(?:id|secret))?|security[_-]?token|x-acs-security-token|authorization|password|signature|secret)\b["']?\s*[:=]\s*["']?[^\s,"';]+`)
	odpsBearerPattern     = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+`)
	odpsOpaqueKeyPattern  = regexp.MustCompile(`\b(?:LTAI|CAIS|AKIA)[A-Za-z0-9+/=]{8,}`)
	odpsRAMPrincipal      = regexp.MustCompile(`(?i)\bRAM\$[0-9]+:[A-Za-z0-9_.@-]*`)
	odpsRAMArn            = regexp.MustCompile(`(?i)\bacs:ram::[0-9]+:[A-Za-z0-9._/-]*`)
)

// ValidateEndpoint checks the credential-bearing project endpoint before
// credentials are attached to an SDK client.
func ValidateEndpoint(project, region, endpoint string) error {
	if !odpsProjectName.MatchString(project) || !odpsRegionName.MatchString(region) {
		return errors.New("invalid ODPS project or region")
	}
	allowedEndpoint := "https://service." + region + ".maxcompute.aliyun.com/api"
	if endpoint != allowedEndpoint {
		return errors.New("invalid ODPS endpoint")
	}
	return nil
}

// NewWithEndpoint accepts only the exact public API URL for region.
// Role credentials are checked on every refresh before the SDK signs requests.
func NewWithEndpoint(project, region, endpoint string, credential credentials.Credential) (*odps.Odps, error) {
	if err := ValidateEndpoint(project, region, endpoint); err != nil {
		return nil, err
	}
	if credential == nil {
		return nil, errors.New("ODPS credentials unavailable")
	}
	instance := odps.NewOdps(account.NewStsAccountWithCredential(guardCredentials(credential), region), endpoint)
	configureODPSClient(instance, project, region)
	return instance, nil
}

// NewWithAccessKey uses the SDK's non-STS account for static keys.
func NewWithAccessKey(project, region, endpoint, id, secret string) (*odps.Odps, error) {
	if err := ValidateEndpoint(project, region, endpoint); err != nil {
		return nil, err
	}
	if strings.TrimSpace(id) == "" || strings.TrimSpace(secret) == "" {
		return nil, errors.New("ODPS credentials unavailable")
	}
	instance := odps.NewOdps(account.NewAliyunAccount(id, secret, region), endpoint)
	configureODPSClient(instance, project, region)
	return instance, nil
}

func configureODPSClient(instance *odps.Odps, project, region string) {
	instance.SetDefaultProjectName(project)
	instance.Options.RegionId = region
}

// SafeError deliberately does not wrap arbitrary SDK errors: HTTP bodies,
// task text and request URLs can contain credentials and SQL literals.
func SafeError(err error) error {
	if err == nil {
		return nil
	}
	// HttpError.Error() includes its raw body and request metadata. Read only
	// structured code/message fields, or parse those fields from the body.
	if httpErr, ok := errors.AsType[restclient.HttpError](err); ok {
		return odpsHTTPStatusError(httpErr.StatusCode, httpErr.ErrorMessage, httpErr.Body)
	}
	if httpErr, ok := errors.AsType[*restclient.HttpError](err); ok && httpErr != nil {
		return odpsHTTPStatusError(httpErr.StatusCode, httpErr.ErrorMessage, httpErr.Body)
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return errors.New("ODPS request timed out")
	}
	// The SDK returns failed SQL task text as a plain error. Preserve the ODPS
	// diagnostic after removing credential-bearing content.
	return odpsErrorCode(err.Error())
}

func odpsHTTPStatusError(status int, message *restclient.ErrorMessage, body []byte) error {
	if message == nil {
		message = restclient.NewErrorMessage(body)
	}
	var base error
	switch status {
	case 401, 403:
		base = errors.New("ODPS authentication or permission denied")
	case 404:
		base = errors.New("ODPS resource not found")
	case 429, 503:
		base = errors.New("ODPS service temporarily unavailable")
	}
	if message != nil {
		if base == nil {
			base = odpsKnownCode(message.ErrorCode)
		}
		if diagnostic := odpsDiagnostic(message.ErrorCode, message.Message); diagnostic != "" {
			if base == nil {
				base = odpsStatusError(status)
			}
			return fmt.Errorf("%w: %s", base, diagnostic)
		}
	}
	if base != nil {
		return base
	}
	return odpsStatusError(status)
}

func odpsStatusError(status int) error {
	if status < 100 || status > 599 {
		return errors.New("ODPS request failed")
	}
	return fmt.Errorf("ODPS request failed (status %d)", status)
}

func odpsErrorCode(text string) error {
	if diagnostic := odpsDiagnostic("", text); diagnostic != "" {
		return fmt.Errorf("ODPS query failed: %s", diagnostic)
	}
	if classified := odpsKnownCode(text); classified != nil {
		return classified
	}
	return errors.New("ODPS request failed")
}

func odpsDiagnostic(code, message string) string {
	odpsCode := strings.ToUpper(odpsErrorCodePattern.FindString(code + " " + message))
	if odpsCode == "" {
		if !odpsNamedCodePattern.MatchString(code) || odpsOpaqueKeyPattern.MatchString(code) {
			return ""
		}
		odpsCode = code
	}
	if index := strings.Index(strings.ToUpper(message), odpsCode); index >= 0 {
		message = strings.TrimLeft(message[index+len(odpsCode):], ": \t")
	}
	message = strings.TrimSpace(strings.SplitN(strings.SplitN(message, "\n", 2)[0], "\r", 2)[0])
	if index := strings.Index(strings.ToLower(message), "tips:"); index >= 0 {
		message = strings.TrimSpace(message[:index])
	}
	// An embedded request dump is not an ODPS diagnostic. Keep only its code.
	lower := strings.ToLower(message)
	if strings.HasPrefix(message, "{") || strings.HasPrefix(message, "<") ||
		strings.Contains(lower, "request body") || strings.Contains(lower, "request headers") {
		return odpsCode
	}
	message = odpsURLPattern.ReplaceAllString(message, "[URL]")
	message = odpsBearerPattern.ReplaceAllString(message, "[credential]")
	message = odpsCredentialPattern.ReplaceAllString(message, "[credential]")
	message = odpsOpaqueKeyPattern.ReplaceAllString(message, "[credential]")
	message = odpsRAMPrincipal.ReplaceAllString(message, "[principal]")
	message = odpsRAMArn.ReplaceAllString(message, "[principal]")
	if len(message) > 512 {
		runes := []rune(message)
		if len(runes) > 512 {
			message = string(runes[:512])
		}
	}
	if message == "" {
		return odpsCode
	}
	return odpsCode + ": " + message
}

func odpsKnownCode(text string) error {
	text = strings.ToLower(text)
	switch {
	case strings.Contains(text, "nopermission"):
		return errors.New("NoPermission: ODPS authentication or permission denied")
	case strings.Contains(text, "invalidaccesskeyid"):
		return errors.New("InvalidAccessKeyId: ODPS authentication or permission denied")
	case strings.Contains(text, "signaturedoesnotmatch"):
		return errors.New("SignatureDoesNotMatch: ODPS authentication or permission denied")
	case strings.Contains(text, "accessdenied"), strings.Contains(text, "permission denied"),
		strings.Contains(text, "not authorized"), strings.Contains(text, "invalid accesskey"),
		strings.Contains(text, "signaturedoesnotmatch"):
		return errors.New("ODPS authentication or permission denied")
	case strings.Contains(text, "projectnotfound"), strings.Contains(text, "tablenotfound"),
		strings.Contains(text, "schemanotfound"):
		return errors.New("ODPS resource not found")
	}
	return nil
}
