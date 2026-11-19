// Package maxcompute is the sqlconnect-go driver for Alibaba Cloud MaxCompute
// (ODPS), built for the MaxCompute reverse ETL source.
package maxcompute

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/driver"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcsql"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/odpsclient"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
)

const (
	// DatabaseType is the name the driver registers under.
	DatabaseType = "maxcompute"

	// connectionTimeout bounds each REST call and TCP connect, as the
	// destination's applyConnectionTimeout does.
	connectionTimeout = 30 * time.Second
)

// DB is a MaxCompute database.
type DB struct {
	*base.DB
	config Config
	client *odps.Odps
}

var _ sqlconnect.QueryController = (*DB)(nil)

// terminateWait bounds StopQuery's wait for a terminated job to end.
const terminateWait = 30 * time.Second

// StopQuery terminates the job whose instance id is queryID and waits for it
// to end. A job that already ended, a second stop included, is StopMissed.
// Instance ids come from maxcomputequery.WithInstanceIDHandler.
func (db *DB) StopQuery(ctx context.Context, queryID string) (sqlconnect.StopOutcome, error) {
	return driver.StopInstance(ctx, db.client, queryID, terminateWait)
}

// NewDB creates a MaxCompute database from its JSON config. It makes no
// network call; Ping does.
func NewDB(configJSON json.RawMessage) (*DB, error) {
	var config Config
	if err := config.Parse(configJSON); err != nil {
		return nil, err
	}
	client, err := odpsclient.NewWithAccessKey(config.Project, config.Region(), config.Endpoint, config.AccessKeyID, config.AccessKeySecret)
	if err != nil {
		return nil, err
	}
	client.SetHttpTimeout(connectionTimeout)
	client.SetTcpConnectTimeout(connectionTimeout)
	// The SDK keeps one HTTP client, and so one connection pool, per Odps
	// value. Submits get their own Odps, made anew after 20 s idle, so a
	// submit never reuses a connection the load balancer dropped.
	newSubmitClient := func() (*odps.Odps, error) {
		submit, err := odpsclient.NewWithAccessKey(config.Project, config.Region(), config.Endpoint, config.AccessKeyID, config.AccessKeySecret)
		if err != nil {
			return nil, err
		}
		submit.SetHttpTimeout(connectionTimeout)
		submit.SetTcpConnectTimeout(connectionTimeout)
		return submit, nil
	}
	connector, err := driver.NewConnector(driver.Config{Client: client, NewSubmitClient: newSubmitClient, DefaultSchema: config.Schema})
	if err != nil {
		return nil, err
	}
	return &DB{
		DB: base.NewDB(
			sql.OpenDB(connector),
			sshtunnel.NoTunnelCloser,
			base.WithDialect(newDialect(mcsql.Normalise(config.Project))),
			base.WithColumnTypeMapper(columnTypeMapper),
			base.WithJsonRowMapper(jsonRowMapper),
			base.WithSQLCommandsOverride(func(cmds base.SQLCommands) base.SQLCommands {
				cmds.CreateTestTable = createTestTableSQL
				return cmds
			}),
		),
		config: config,
		client: client,
	}, nil
}

// createTestTableSQL is the connection test's table. LIFECYCLE 1 keeps the
// CREATE valid on a project with odps.table.lifecycle=mandatory, where a
// table without a lifecycle is refused (findings 18), and lets a table a
// failed validation leaked expire on its own.
func createTestTableSQL(table base.QuotedIdentifier) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (c1 INT, c2 VARCHAR(255)) LIFECYCLE 1", table)
}

func init() {
	sqlconnect.RegisterDBFactory(DatabaseType, func(credentialsJSON json.RawMessage) (sqlconnect.DB, error) {
		return NewDB(credentialsJSON)
	})
}
