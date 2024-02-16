package postgres

import (
	"database/sql"
	"encoding/json"

	_ "github.com/lib/pq" // postgres driver
	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

const (
	DatabaseType        = "postgres"
	defaultRudderSchema = "_rudderstack"
)

// NewDB creates a new postgres-specific client
func NewDB(credentialsJSON json.RawMessage) (*DB, error) {
	var config Config
	err := config.Parse(credentialsJSON)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(DatabaseType, config.ConnectionString())
	if err != nil {
		return nil, err
	}

	return &DB{
		DB: base.NewDB(
			db,
			lo.Ternary(config.RudderSchema != "", config.RudderSchema, defaultRudderSchema),
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
