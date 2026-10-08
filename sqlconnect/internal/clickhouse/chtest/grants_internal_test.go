package chtest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScopedUserStatements_CustomerGrantSet(t *testing.T) {
	grants := []string{
		"GRANT SELECT ON `customer_db`.* TO `rudder_retl`",
		"GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON `_rudderstack`.* TO `rudder_retl`",
		"GRANT ALTER DELETE ON `_rudderstack`.`sync_log` TO `rudder_retl`",
		"GRANT SELECT ON system.processes TO `rudder_retl`",
		"GRANT SELECT ON system.query_log TO `rudder_retl`",
	}
	head := []string{
		"CREATE USER `rudder_retl` IDENTIFIED WITH sha256_password BY 'pw_Retl_123'",
		"CREATE DATABASE IF NOT EXISTS `_rudderstack`",
		"CREATE DATABASE IF NOT EXISTS `customer_db`",
	}
	want := append(append([]string{}, head...), grants...)
	require.Equal(t, want, scopedUserStatements("rudder_retl", "pw_Retl_123", "customer_db", "_rudderstack", false),
		"the sync_log grant is part of the customer set, with or without the table")
	require.Equal(t, append(want, "CREATE TABLE IF NOT EXISTS `_rudderstack`.`sync_log` (id UInt8) ENGINE = MergeTree ORDER BY id"),
		scopedUserStatements("rudder_retl", "pw_Retl_123", "customer_db", "_rudderstack", true))
}
