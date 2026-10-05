package cherr_test

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

func TestLiteralCategories(t *testing.T) {
	// This fixture is a literal contract, never generated from Category or Codes.
	b, err := os.ReadFile("testdata/categories.json")
	require.NoError(t, err)
	var want map[string]string
	require.NoError(t, json.Unmarshal(b, &want))
	require.Len(t, want, 38)
	var codes []string
	for code, category := range want {
		codes = append(codes, code)
		t.Run(code, func(t *testing.T) { require.Equal(t, category, cherr.Category(code)) })
	}
	slices.Sort(codes)
	require.Equal(t, codes, cherr.Codes())
	for _, unknown := range []string{"", "CH_NEW_CODE", "CH_TIMEOUT ", "ch_timeout"} {
		require.Equal(t, "unknown", cherr.Category(unknown))
	}
	// The returned inventory must not permit a caller to alter the registry.
	copyOfCodes := cherr.Codes()
	copyOfCodes[0] = "CH_TAMPERED"
	require.Equal(t, codes, cherr.Codes())
}

func FuzzCherrFormatting(f *testing.F) {
	f.Add([]byte("password='customer-value'"))
	f.Add([]byte{0, 10, 255})
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 4096 {
			t.Skip()
		}
		secret := "p15-secret-" + hex.EncodeToString(payload) + "-end"
		raw := errors.New(secret)
		e := cherr.Wrap("CH_AUTHENTICATION", "", "credentials refused", raw)
		// cherr.Wrap itself deliberately retains the cause; the driver must redact it.
		require.ErrorIs(t, e, raw)
		for _, verb := range []string{"%s", "%v", "%+v", "%#v", "%q"} {
			require.False(t, strings.Contains(fmt.Sprintf(verb, e), secret), verb)
		}
		b, err := json.Marshal(e)
		require.NoError(t, err)
		require.NotContains(t, string(b), secret)
	})
}
