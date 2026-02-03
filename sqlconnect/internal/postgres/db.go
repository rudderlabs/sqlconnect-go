package postgres

import (
	"database/sql"
	"encoding/json"

	_ "github.com/lib/pq" // postgres driver

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
)

const (
	DatabaseType = "postgres"
)

// NewDB creates a new postgres db client
func NewDB(credentialsJSON json.RawMessage) (*DB, error) {
	var config Config
	err := config.Parse(credentialsJSON)
	if err != nil {
		return nil, err
	}

	tunnelCloser := sshtunnel.NoTunnelCloser
	if config.TunnelInfo != nil {
		tunnel, err := sshtunnel.NewTcpTunnel(*config.TunnelInfo, config.Host, config.Port)
		if err != nil {
			return nil, err
		}
		tunnelCloser = tunnel.Close
		// Update the remote host and port to the tunnel's host and port
		config.Host = tunnel.Host()
		config.Port = tunnel.Port()
	}

	db, err := sql.Open(DatabaseType, config.ConnectionString())
	if err != nil {
		return nil, err
	}

	return &DB{
		DB: base.NewDB(
			db,
			tunnelCloser,
			base.WithDialect(NewDialect()),
			base.WithColumnTypeMappings(getColumnTypeMappings(config)),
			base.WithJsonRowMapper(getJonRowMapper(config)),
		),
	}, nil
}

func init() {
	sqlconnect.RegisterDBFactory(DatabaseType, func(credentialsJSON json.RawMessage) (sqlconnect.DB, error) {
		return NewDB(credentialsJSON)
	})
}

type DB struct {
	*base.DB
}

func getColumnTypeMappings(config Config) map[string]string {
	if config.UseLegacyMappings {
		return legacyColumnTypeMappings
	}
	return columnTypeMappings
}

func getJonRowMapper(config Config) func(databaseTypeName string, value any) any {
	if config.UseLegacyMappings {
		return legacyJsonRowMapper
	}
	return jsonRowMapper
}
