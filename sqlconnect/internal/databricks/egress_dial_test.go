package databricks_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/databricks"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

func TestEgressPolicyBlocksDialedIP(t *testing.T) {
	const target = "192.0.2.10" // RFC 5737 TEST-NET-1; blocked only by the explicit /32 below
	db, err := databricks.NewDB([]byte(`{"host":"192.0.2.10","port":443,"path":"/sql/1.0/warehouses/x","token":"t","retryAttempts":1}`))
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
