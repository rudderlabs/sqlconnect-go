package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestMoveCopyBeforeDrop(t *testing.T) {
	for _, tc := range []struct {
		name           string
		failStage      string
		fault          error
		wantCode       string
		wantSteps      []string
		explicitColumn string
	}{
		{"success", "", nil, "", []string{"describe", "create", "visible", "describe", "insert", "drop"}, ""},
		{"describe_denied", "describe", &ch.Exception{Code: 497}, "CH_PERMISSION", []string{"describe"}, ""},
		{"create_denied", "create", &ch.Exception{Code: 497}, "CH_PERMISSION", []string{"describe", "create"}, ""},
		{"visibility_denied", "visible", &ch.Exception{Code: 497}, "CH_PERMISSION", []string{"describe", "create", "visible"}, ""},
		{"copy_resource_limit", "insert", &ch.Exception{Code: 241}, "CH_RESOURCE", []string{"describe", "create", "visible", "describe", "insert"}, ""},
		{"copy_timeout", "insert", &ch.Exception{Code: 159}, "CH_TIMEOUT", []string{"describe", "create", "visible", "describe", "insert"}, ""},
		{"copy_cancelled", "insert", context.Canceled, "CH_CANCELLED", []string{"describe", "create", "visible", "describe", "insert"}, ""},
		{"copy_lost_response", "insert", io.ErrUnexpectedEOF, "CH_NETWORK", []string{"describe", "create", "visible", "describe", "insert"}, ""},
		{"drop_denied", "drop", &ch.Exception{Code: 497}, "CH_PERMISSION", []string{"describe", "create", "visible", "describe", "insert", "drop"}, ""},
		{"explicit_match", "", nil, "", []string{"create", "visible", "describe", "insert", "drop"}, "a"},
		{"projection_mismatch", "", nil, "CH_SCHEMA_MISMATCH", []string{"create", "visible", "describe"}, "b"},
		{"explicit_describe_denied", "describe", &ch.Exception{Code: 497}, "CH_PERMISSION", []string{"create", "visible", "describe"}, "a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var steps []string
			var visibilityReads int
			stage := func(name string) error {
				steps = append(steps, name)
				if tc.failStage == name {
					return tc.fault
				}
				return nil
			}
			pool := sql.OpenDB(stubConnector{
				query: func(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
					if strings.HasPrefix(q, "DESCRIBE (") {
						require.Contains(t, q, "SELECT * FROM `scratch`.`old`")
						return &describeRows{}, stage("describe")
					}
					if q == visibilitySQL {
						visibilityReads++
						if visibilityReads > 1 {
							return nil, fmt.Errorf("unexpected visibility reread after returning the table UUID")
						}
						return &stubRows{vals: []string{p07WantUUID}}, stage("visible")
					}
					return nil, fmt.Errorf("unexpected query: %s", q)
				},
				exec: func(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
					switch {
					case strings.HasPrefix(q, "CREATE TABLE `scratch`.`new` "):
						return driver.RowsAffected(0), stage("create")
					case strings.HasPrefix(q, "INSERT INTO `scratch`.`new` "):
						require.Contains(t, q, "SELECT * FROM `scratch`.`old`")
						return driver.RowsAffected(0), stage("insert")
					case q == "DROP TABLE IF EXISTS `scratch`.`old` SYNC":
						return driver.RowsAffected(0), stage("drop")
					default:
						return nil, fmt.Errorf("unexpected write: %s", q)
					}
				},
			})
			t.Cleanup(func() { _ = pool.Close() })
			conn, err := pool.Conn(context.Background())
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			opts := sqlconnect.MaterializationOptions{}
			if tc.explicitColumn != "" {
				opts.Columns = []sqlconnect.ColumnRef{{Name: tc.explicitColumn, RawType: "UInt8"}}
			}
			var admin sqlconnect.MaterializationAdmin = unitDB(t)
			got, err := admin.MoveTableWithOptions(context.Background(), conn,
				sqlconnect.NewRelationRef("old", sqlconnect.WithSchema("scratch")),
				sqlconnect.NewRelationRef("new", sqlconnect.WithSchema("scratch")), opts)
			if tc.wantCode == "" {
				require.NoError(t, err)
				require.Equal(t, p07WantUUID, got)
			} else {
				requireCode(t, err, tc.wantCode)
				require.Empty(t, got, "failed materialization must not publish a UUID")
				require.Equal(t, tc.failStage == "drop", errors.Is(err, sqlconnect.ErrDropOldTablePostCopy),
					"only a completed copy followed by a failed DROP carries the post-copy sentinel")
			}
			require.Equal(t, tc.wantSteps, steps, "copy failures must neither retry INSERT nor send DROP")
		})
	}
}
