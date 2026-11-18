// Package maxcompute is the sqlconnect-go driver for Alibaba Cloud MaxCompute
// (ODPS), built for the MaxCompute reverse ETL source.
package maxcompute

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/driver"
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
	connector, err := driver.NewConnector(driver.Config{Client: client, DefaultSchema: config.Schema})
	if err != nil {
		return nil, err
	}
	return &DB{
		DB:     base.NewDB(sql.OpenDB(connector), sshtunnel.NoTunnelCloser),
		config: config,
	}, nil
}

func init() {
	sqlconnect.RegisterDBFactory(DatabaseType, func(credentialsJSON json.RawMessage) (sqlconnect.DB, error) {
		return NewDB(credentialsJSON)
	})
}
