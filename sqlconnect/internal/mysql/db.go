package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

const (
	DatabaseType        = "mysql"
	defaultRudderSchema = "_rudderstack"
)

// NewDB creates a new postgres-specific client
func NewDB(configJSON json.RawMessage) (*DB, error) {
	var config Config
	err := config.Parse(configJSON)
	if err != nil {
		return nil, err
	}

	connectionString, err := config.ConnectionString()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(DatabaseType, connectionString)
	if err != nil {
		return nil, err
	}

	return &DB{
		DB: base.NewDB(
			db,
			lo.Ternary(config.RudderSchema != "", config.RudderSchema, defaultRudderSchema),
			base.WithColumnTypeMapper(getColumnTypeMapper(config)),
			base.WithJsonRowMapper(getJonRowMapper(config)),
			base.WithSQLCommandsOverride(func(cmds base.SQLCommands) base.SQLCommands {
				cmds.DropSchema = func(schema string) string { // mysql does not support CASCADE
					return fmt.Sprintf("DROP SCHEMA %[1]s", schema)
				}
				cmds.RenameTable = func(schema, oldName, newName string) string {
					return fmt.Sprintf("RENAME TABLE %[1]s.%[2]s TO %[1]s.%[3]s", schema, oldName, newName)
				}
				return cmds
			}),
			base.WithDialect(dialect{}),
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

func getColumnTypeMapper(config Config) func(base.ColumnType) string {
	if config.UseLegacyMappings {
		return columnTypeMapper(nil)
	}
	return columnTypeMapper(columnTypeMappings)
}

func getJonRowMapper(config Config) func(databaseTypeName string, value any) any {
	if config.UseLegacyMappings {
		return legacyJsonRowMapper
	}
	return jsonRowMapper
}
