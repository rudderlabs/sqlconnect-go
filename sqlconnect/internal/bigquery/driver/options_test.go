package driver

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func optionTypeNames(opts []option.ClientOption) []string {
	names := make([]string, len(opts))
	for i, o := range opts {
		names[i] = fmt.Sprintf("%T", o)
	}
	return names
}

func containsCredentials(name string) bool {
	return strings.Contains(strings.ToLower(name), "credentials")
}

func containsWithoutAuth(name string) bool {
	return strings.Contains(strings.ToLower(name), "withoutauthentication")
}

func containsScopes(name string) bool {
	return strings.Contains(strings.ToLower(name), "scopes")
}

func containsEndpoint(name string) bool {
	return strings.Contains(strings.ToLower(name), "endpoint")
}

func TestOptionsFor(t *testing.T) {
	t.Run("disable_auth=true skips credentials even when both are set", func(t *testing.T) {
		opts := optionsFor(&bigQueryConfig{
			disableAuth:     true,
			credentialFile:  "/some/file",
			credentialsJSON: "irrelevant",
		})
		names := optionTypeNames(opts)
		require.Len(t, names, 2, "expected exactly scopes + WithoutAuthentication, got %v", names)
		require.True(t, containsScopes(names[0]), "first option must be a scopes option, got %s", names[0])
		require.True(t, containsWithoutAuth(names[1]), "second option must be a no-auth option, got %s", names[1])
		for _, n := range names {
			require.False(t, containsCredentials(n), "no credential option must be present, found %s", n)
		}
	})

	t.Run("credentials_json alone produces an auth-credentials-JSON option", func(t *testing.T) {
		opts := optionsFor(&bigQueryConfig{
			credentialsJSON: "{}",
		})
		names := optionTypeNames(opts)
		require.Len(t, names, 2, "expected scopes + auth-credentials-json, got %v", names)
		require.True(t, containsScopes(names[0]), "first option must be a scopes option, got %s", names[0])
		require.True(t, containsCredentials(names[1]), "second option must be a credentials option, got %s", names[1])
		require.Contains(t, strings.ToLower(names[1]), "json", "expected JSON credentials option, got %s", names[1])
		for _, n := range names {
			require.False(t, containsWithoutAuth(n), "WithoutAuthentication must not be present, found %s", n)
		}
	})

	t.Run("credential_file alone produces an auth-credentials-file option", func(t *testing.T) {
		opts := optionsFor(&bigQueryConfig{
			credentialFile: "/x",
		})
		names := optionTypeNames(opts)
		require.Len(t, names, 2, "expected scopes + auth-credentials-file, got %v", names)
		require.True(t, containsScopes(names[0]), "first option must be a scopes option, got %s", names[0])
		require.True(t, containsCredentials(names[1]), "second option must be a credentials option, got %s", names[1])
		require.Contains(t, strings.ToLower(names[1]), "file", "expected file credentials option, got %s", names[1])
		for _, n := range names {
			require.False(t, containsWithoutAuth(n), "WithoutAuthentication must not be present, found %s", n)
		}
	})

	t.Run("endpoint adds an endpoint option", func(t *testing.T) {
		opts := optionsFor(&bigQueryConfig{
			endpoint:        "http://localhost:9050",
			credentialsJSON: "{}",
		})
		names := optionTypeNames(opts)
		require.Len(t, names, 3, "expected scopes + endpoint + auth-credentials-json, got %v", names)
		require.True(t, containsScopes(names[0]), "first option must be a scopes option, got %s", names[0])
		require.True(t, containsEndpoint(names[1]), "second option must be an endpoint option, got %s", names[1])
		require.True(t, containsCredentials(names[2]), "third option must be a credentials option, got %s", names[2])
	})
}
