package redshift

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/lib/pq" // postgres driver

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/postgres"
)

const (
	DatabaseType = "redshift"
)

// NewDB creates a new redshift db client
func NewDB(credentialsJSON json.RawMessage) (*DB, error) {
	var config Config
	err := config.Parse(credentialsJSON)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(postgres.DatabaseType, config.ConnectionString())
	if err != nil {
		return nil, err
	}

	return &DB{
		DB: base.NewDB(
			db,
			base.WithColumnTypeMappings(getColumnTypeMappings(config)),
			base.WithJsonRowMapper(getJonRowMapper(config)),
			base.WithSQLCommandsOverride(func(cmds base.SQLCommands) base.SQLCommands {
				cmds.CurrentCatalog = func() string {
					return "SELECT current_database()"
				}
				cmds.ListSchemas = func() (string, string) {
					return "SELECT schema_name FROM svv_redshift_schemas", "schema_name"
				}
				cmds.SchemaExists = func(schema base.UnquotedIdentifier) string {
					return fmt.Sprintf("SELECT schema_name FROM svv_redshift_schemas WHERE schema_name = '%[1]s'", schema)
				}
				return cmds
			}),
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

func getColumnTypeMappings(config postgres.Config) map[string]string {
	if config.UseLegacyMappings {
		return legacyColumnTypeMappings
	}
	return columnTypeMappings
}

func getJonRowMapper(config postgres.Config) func(databaseTypeName string, value any) any {
	if config.UseLegacyMappings {
		return legacyJsonRowMapper
	}
	return jsonRowMapper
}
