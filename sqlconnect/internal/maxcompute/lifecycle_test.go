package maxcompute

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func TestLifecycleDDL(t *testing.T) {
	require.Equal(t, "CREATE TABLE IF NOT EXISTS `p`.`s`.`t` (c1 INT, c2 VARCHAR(255)) LIFECYCLE 1",
		createTestTableSQL(base.QuotedIdentifier("`p`.`s`.`t`")), "the connection test table passes on a mandatory-lifecycle project")

	require.Equal(t, "CREATE TABLE `t` LIFECYCLE 3650 AS SELECT 1", ctasSQL("`t`", 3650, "SELECT 1"), "LIFECYCLE goes before AS in a CTAS")
	require.Equal(t, "CREATE TABLE `t` AS SELECT 1", ctasSQL("`t`", 0, "SELECT 1"))

	ctx, err := WithLifecycle(context.Background(), 3650)
	require.NoError(t, err)
	require.Equal(t, 3650, lifecycleFrom(ctx))
	require.Zero(t, lifecycleFrom(context.Background()))
	for _, bad := range []int{0, -1, 37232} {
		_, err := WithLifecycle(context.Background(), bad)
		require.ErrorIs(t, err, ErrLifecycle, bad)
	}
	for _, ok := range []int{1, 37231} {
		_, err := WithLifecycle(context.Background(), ok)
		require.NoError(t, err, ok)
	}
}
