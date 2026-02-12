package snowflake

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/samber/lo"
	_ "github.com/snowflakedb/gosnowflake" // snowflake driver

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
)

const (
	DatabaseType = "snowflake"
)

// NewDB creates a new snowflake db client
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
			sshtunnel.NoTunnelCloser,
			base.WithDialect(newDialect()),
			base.WithColumnTypeMapper(getColumnTypeMapper(config)),
			base.WithJsonRowMapper(getJonRowMapper(config)),
			base.WithSQLCommandsOverride(func(cmds base.SQLCommands) base.SQLCommands {
				cmds.CurrentCatalog = func() string {
					return "SELECT current_database()"
				}
				cmds.ListCatalogs = func() (string, string) {
					return "SHOW TERSE DATABASES", "name"
				}
				cmds.ListSchemas = func() (string, string) { return "SHOW TERSE SCHEMAS", "name" }
				cmds.SchemaExists = func(schema base.UnquotedIdentifier) string {
					return fmt.Sprintf("SHOW TERSE SCHEMAS LIKE '%[1]s'", base.EscapeSqlString(schema))
				}
				cmds.ListTables = func(schema base.UnquotedIdentifier) []lo.Tuple2[string, string] {
					return []lo.Tuple2[string, string]{
						{A: fmt.Sprintf(`SHOW TERSE TABLES IN SCHEMA "%[1]s"`, schema), B: "name"},
						{A: fmt.Sprintf(`SHOW TERSE VIEWS IN SCHEMA "%[1]s"`, schema), B: "name"},
					}
				}
				cmds.ListTablesWithPrefix = func(schema base.UnquotedIdentifier, prefix string) []lo.Tuple2[string, string] {
					return []lo.Tuple2[string, string]{
						{A: fmt.Sprintf(`SHOW TERSE TABLES LIKE '%[2]s' IN SCHEMA "%[1]s"`, schema, prefix+"%"), B: "name"},
					}
				}
				cmds.TableExists = func(schema, table base.UnquotedIdentifier) string {
					return fmt.Sprintf("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%[1]s' AND TABLE_NAME = '%[2]s'", base.EscapeSqlString(schema), base.EscapeSqlString(table))
				}
				cmds.ListColumns = func(catalog, schema, table base.UnquotedIdentifier) (string, string, string) {
					if catalog != "" {
						return fmt.Sprintf(`DESCRIBE TABLE "%[1]s"."%[2]s"."%[3]s"`, catalog, schema, table), "name", "type"
					}
					return fmt.Sprintf(`DESCRIBE TABLE "%[1]s"."%[2]s"`, schema, table), "name", "type"
				}
				cmds.RenameTable = func(schema, oldName, newName base.QuotedIdentifier) string {
					return fmt.Sprintf(`ALTER TABLE %[1]s.%[2]s RENAME TO %[1]s.%[3]s`, schema, oldName, newName)
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

func getColumnTypeMapper(config Config) func(base.ColumnType) string {
	if config.UseLegacyMappings {
		return legacyColumnTypeMapper
	}
	return columnTypeMapper
}

func getJonRowMapper(config Config) func(databaseTypeName string, value any) any {
	if config.UseLegacyMappings {
		return legacyJsonRowMapper
	}
	return jsonRowMapper
}
