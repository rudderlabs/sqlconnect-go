package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
