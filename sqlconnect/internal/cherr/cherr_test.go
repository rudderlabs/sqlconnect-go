package cherr_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

func TestCherr_RegistryCategories(t *testing.T) {
	require.Len(t, cherr.Codes(), 38, "every LLD registry code, listed once in cherr.go")
	for code, category := range map[string]string{
		"CH_TLS": "connection configuration", "CH_RATE_LIMITED": "transient",
		"CH_OBJECT_NOT_FOUND": "not_found", "CH_SCRATCH_CLEANUP_FAILED": "cleanup", "CH_SCHEMA_MISMATCH": "integrity",
		"CH_SORTING_KEY_UNAVAILABLE": "configuration", "CH_UNAVAILABLE": "availability", "CH_NOT_A_CODE": "unknown",
		"CH_LOST_RESPONSE": "integrity", "CH_OUTCOME_UNKNOWN": "integrity",
	} {
		require.Equal(t, category, cherr.Category(code), code)
	}
}

func TestCherr_CodesAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, code := range cherr.Codes() {
		require.False(t, seen[code], code)
		seen[code] = true
	}
}

func TestCherr_CauseNeverPrinted(t *testing.T) {
	cause := errors.New("Code: 516. DB::Exception: sentinel-pw-123")
	err := cherr.Wrap(cherr.CodeAuthentication, "", "the server refused the credentials", cause)
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		require.NotContains(t, fmt.Sprintf(verb, err), "sentinel-pw-123", verb)
	}
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, cherr.Wrap(cherr.CodeCatalogUnsupported, "catalog", "x", sqlconnect.ErrNotSupported), sqlconnect.ErrNotSupported)
}

func TestCherr_ErrorText(t *testing.T) {
	e := cherr.New(cherr.CodePermission, "INSERT", "the user lacks a required privilege")
	require.Equal(t, "CH_PERMISSION (INSERT): the user lacks a required privilege", e.Error())
	e.ServerCode = 497
	require.Equal(t, "CH_PERMISSION (INSERT): the user lacks a required privilege [server code 497]", e.Error())
	require.Equal(t, `"CH_CONFIG_INVALID: x"`, fmt.Sprintf("%q", cherr.New(cherr.CodeConfigInvalid, "", "x")))
}
