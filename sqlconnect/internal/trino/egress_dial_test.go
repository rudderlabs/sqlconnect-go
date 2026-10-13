package trino_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/trino"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

// Without a tunnel, trino now routes through the guarded custom client, so a dial
// to a blocked IP is refused by the egress policy. The target is RFC 5737
// TEST-NET-1: it passes parse-time host validation and the always-blocked
// classes, so it is blocked only by the explicit /32 set here — which is what
// proves the policy reaches trino's HTTP transport.
func TestEgressPolicyBlocksDialedIP(t *testing.T) {
	const target = "192.0.2.10"
	// Open with an empty policy so parse-time host validation passes, then set the
	// policy before dialing. This is the TOCTOU case the dial-time guard exists for:
	// the host was validated benign at parse time, so only the dial-time check can
	// catch it.
	require.NoError(t, sqlconnect.SetEgressPolicy(nil))
	db, err := trino.NewDB([]byte(`{"host":"192.0.2.10","port":443,"catalog":"c","user":"u"}`))
	require.NoError(t, err, "NewDB opens lazily; it must not require a live server")
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, sqlconnect.SetEgressPolicy([]string{target + "/32"}))
	t.Cleanup(func() { _ = sqlconnect.SetEgressPolicy(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	require.Error(t, err, "dialing a blocked IP must fail")

	var blocked *util.BlockedHostError
	require.ErrorAs(t, err, &blocked, "the dial must be refused by the egress guard, got: %v", err)
}
