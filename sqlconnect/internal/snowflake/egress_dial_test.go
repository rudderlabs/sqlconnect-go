package snowflake_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/snowflake"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

func TestConcurrentConnectionOpensKeepEgressGuard(t *testing.T) {
	const workers = 12
	const target = "192.0.2.10" // RFC 5737 TEST-NET-1

	db, err := snowflake.NewDB([]byte(`{"account":"testaccount","user":"u","password":"p","host":"192.0.2.10","port":443,"loginTimeout":2000000000}`))
	require.NoError(t, err)
	require.NoError(t, sqlconnect.SetEgressPolicy([]string{target + "/32"}))
	t.Cleanup(func() {
		_ = db.Close()
		_ = sqlconnect.SetEgressPolicy(nil)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make(chan error, workers)
	for range workers {
		go func() {
			<-start
			results <- db.PingContext(ctx)
		}()
	}
	close(start)
	errs := make([]error, 0, workers)
	for range workers {
		errs = append(errs, <-results)
	}
	for _, err := range errs {
		var blocked *util.BlockedHostError
		require.ErrorAs(t, err, &blocked, "concurrent connection opens must still use the guarded dialer")
	}
}

func TestEgressPolicyBlocksDialedIP(t *testing.T) {
	const target = "192.0.2.10" // RFC 5737 TEST-NET-1; blocked only by the explicit /32 below
	// loginTimeout (nanoseconds) bounds gosnowflake's own auth retry loop so it
	// returns the wrapped dial error quickly. The context deadline is only a
	// safety net and must be longer than loginTimeout, otherwise it pre-empts the
	// retry loop and surfaces as "context deadline exceeded" instead of the
	// BlockedHostError.
	db, err := snowflake.NewDB([]byte(`{"account":"testaccount","user":"u","password":"p","host":"192.0.2.10","port":443,"loginTimeout":2000000000}`))
	require.NoError(t, err, "NewDB opens lazily; it must not require a live server")
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, sqlconnect.SetEgressPolicy([]string{target + "/32"}))
	t.Cleanup(func() { _ = sqlconnect.SetEgressPolicy(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	require.Error(t, err, "dialing a blocked IP must fail")

	var blocked *util.BlockedHostError
	require.ErrorAs(t, err, &blocked, "the dial must be refused by the egress guard, got: %v", err)
}
