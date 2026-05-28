package driver

import (
	"encoding/json"

	"google.golang.org/api/option"
)

// credentialsTypeUnknown disables credential-type validation, matching the
// behaviour of the deprecated WithCredentialsJSON/WithCredentialsFile options.
// The option package does not export an Unknown constant, so it is referenced
// here by its zero value.
const credentialsTypeUnknown = option.CredentialsType("")

// CredentialsTypeFromJSON derives the option.CredentialsType from a Google
// credential JSON payload by inspecting its "type" field. It returns the
// Unknown type (empty string) when the type is missing, unrecognised, or the
// payload is not valid JSON; passing Unknown skips credential-type validation,
// preserving the behaviour of the deprecated credential options.
func CredentialsTypeFromJSON(credsJSON []byte) option.CredentialsType {
	var hdr struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(credsJSON, &hdr); err != nil {
		return credentialsTypeUnknown
	}
	switch hdr.Type {
	case "service_account":
		return option.ServiceAccount
	case "authorized_user":
		return option.AuthorizedUser
	case "impersonated_service_account":
		return option.ImpersonatedServiceAccount
	case "external_account":
		return option.ExternalAccount
	// The option package does not export constants for the two credential types
	// below, so they are referenced by their documented "type" string values.
	case "external_account_authorized_user":
		return option.CredentialsType("external_account_authorized_user")
	case "gdc_service_account":
		return option.CredentialsType("gdc_service_account")
	default:
		return credentialsTypeUnknown
	}
}
