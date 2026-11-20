package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute"
)

func TestMaxComputeSweepable(t *testing.T) {
	now := time.Unix(1795100000, 0)
	old := now.Add(-25 * time.Hour).Unix()
	fresh := now.Add(-time.Hour).Unix()
	for name, want := range map[string]bool{
		fmt.Sprintf("tsqlcon_gppbuorscrfk_%d", old):                           false, // bare: not attributable to this repo
		fmt.Sprintf("rs_retl_probe_sc_e692d7f4_tsqlcon_gppbuorscrfk_%d", old): true,
		fmt.Sprintf("rs_retl_probe_sc_e692d7f4_tsqlcon_meta_%d", old):         true,
		fmt.Sprintf("tsqlcon_gppbuorscrfk_%d", fresh):                         false, // a run in progress
		fmt.Sprintf("rs_retl_probe_sc_e692d7f4_tsqlcon_meta_%d", fresh):       false,
		"rs_retl_probe_sc_e692d7f4_tsqlcon_meta":                              false, // no time: never guess its age
		fmt.Sprintf("rs_retl_probe_s1_tsqlcon_x_%d", old):                     false, // another team's probe prefix
		fmt.Sprintf("rs_retl_probe_src_dbg02_tsqlcon_x_%d", old):              false, // rudder-sources' probes
		"rs_retl_probe_s1":                        false,
		fmt.Sprintf("customer_tsqlcon_x_%d", old): false,
		fmt.Sprintf("tsqlcon_x_%d_suffix", old):   false,
		"default":                                 false,
	} {
		require.Equal(t, want, maxComputeSweepable(name, now), name)
	}
}

func TestCleanupConfigGates(t *testing.T) {
	env := func(vals map[string]string) func(string) string {
		return func(k string) string { return vals[k] }
	}
	mc := cleanupConfig{SkipIfEmpty: true, DropEnv: "MAXCOMPUTE_CLEANUP_DROP"}
	other := cleanupConfig{}

	t.Run("an empty secret skips only a SkipIfEmpty warehouse", func(t *testing.T) {
		require.True(t, mc.skipped(""))
		require.False(t, mc.skipped(`{"project":"p"}`))
		require.False(t, other.skipped(""), "other warehouses still fail on an empty secret")
	})
	t.Run("a DropEnv warehouse drops only when it is true", func(t *testing.T) {
		require.True(t, mc.dryRun(env(nil)))
		require.True(t, mc.dryRun(env(map[string]string{"MAXCOMPUTE_CLEANUP_DROP": "1"})))
		require.True(t, mc.dryRun(env(map[string]string{"MAXCOMPUTE_CLEANUP_DROP": "TRUE"})))
		require.False(t, mc.dryRun(env(map[string]string{"MAXCOMPUTE_CLEANUP_DROP": "true"})))
		require.False(t, other.dryRun(env(nil)), "other warehouses drop as before")
	})
	t.Run("the MaxCompute entry carries both gates and its own sweep", func(t *testing.T) {
		c := maxComputeCleanupConfig()
		require.Equal(t, "MAXCOMPUTE_TEST_ENVIRONMENT_CREDENTIALS", c.Env)
		require.Equal(t, maxcompute.DatabaseType, c.Type)
		require.True(t, c.SkipIfEmpty)
		require.Equal(t, "MAXCOMPUTE_CLEANUP_DROP", c.DropEnv)
		require.NotNil(t, c.Sweep, "nil would fall back to the default tsqlcon_ sweep")
		old := time.Now().Add(-25 * time.Hour).Unix()
		require.True(t, c.Sweep(fmt.Sprintf("rs_retl_probe_sc_e692d7f4_tsqlcon_meta_%d", old)))
		require.False(t, c.Sweep(fmt.Sprintf("tsqlcon_gppbuorscrfk_%d", old)), "a bare tsqlcon_ name is not ours")
	})
}
