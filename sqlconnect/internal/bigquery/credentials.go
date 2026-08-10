package bigquery

import (
	"encoding/json"
	"fmt"
	"strings"
)

// serviceAccountCredentialType is the only Google credential type accepted.
const serviceAccountCredentialType = "service_account"

// validateServiceAccountJSON returns an error unless jsonKey is a Google
// service account credentials document.
//
// Not every Google credential type is a key. Some are instruction documents
// that the SDK acts on: external_account carries a credential_source naming a
// local file to read and a token_url naming where to send the contents, and
// impersonated_service_account carries a service_account_impersonation_url.
// Accepting those from caller-supplied config turns a connector into a file
// read with exfiltration attached, so the type is allow-listed here.
//
// This has to be enforced in first-party code. Passing
// option.WithAuthCredentialsJSON(option.ServiceAccount, ...) looks like it
// constrains the type, but supplying that option is what enables Google's
// newer auth stack, which drops the credential type before the SDK's own
// check would run.
// Empty credentials are left alone deliberately: with no credentials JSON the
// Google SDK falls back to application default credentials, which is how
// workload-identity deployments authenticate. Rejecting empty here would break
// them. Only a supplied document is checked.
func validateServiceAccountJSON(jsonKey []byte) error {
	if isEmptyCredentials(jsonKey) {
		return nil
	}
	var f struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(jsonKey, &f); err != nil {
		return fmt.Errorf("invalid credentials json: %w", err)
	}
	if f.Type != serviceAccountCredentialType {
		return fmt.Errorf("unsupported credential type %q: only service account credentials are supported", f.Type)
	}
	return nil
}

// isEmptyCredentials reports whether the credentials field carries no document.
// Both an empty string and "{}" are used to mean "authenticate some other way".
func isEmptyCredentials(jsonKey []byte) bool {
	trimmed := strings.TrimSpace(string(jsonKey))
	return trimmed == "" || trimmed == "{}"
}
