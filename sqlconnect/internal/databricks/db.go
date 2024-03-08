package databricks

import (
	"database/sql"
	"encoding/json"
	"fmt"

	databricks "github.com/databricks/databricks-sql-go"
	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

const (
	DatabaseType        = "databricks"
	defaultRudderSchema = "_rudderstack"
)

// NewDB creates a new databricks db client
func NewDB(configJson json.RawMessage) (*DB, error) {
	var config Config
	err := config.Parse(configJson)
	if err != nil {
		return nil, err
	}

	connector, err := databricks.NewConnector(
		databricks.WithAccessToken(config.Token),
		databricks.WithServerHostname(config.Host),
		databricks.WithPort(config.Port),
		databricks.WithHTTPPath(config.Path),
		databricks.WithInitialNamespace(config.Catalog, ""),
		databricks.WithRetries(
			config.RetryAttempts,
			config.MinRetryWaitTime,
			config.MaxRetryWaitTime,
		),
		databricks.WithUserAgentEntry("Rudderstack"),
	)
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)
	db.SetConnMaxIdleTime(config.MaxConnIdleTime)

	return &DB{
		DB: base.NewDB(
			db,
			lo.Ternary(config.RudderSchema != "", config.RudderSchema, defaultRudderSchema),
			base.WithDialect(dialect{}),
			base.WithColumnTypeMapper(getColumnTypeMapper(config)),
			base.WithJsonRowMapper(getJonRowMapper(config)),
			base.WithSQLCommandsOverride(func(cmds base.SQLCommands) base.SQLCommands {
				cmds.ListSchemas = func() (string, string) { return "SHOW SCHEMAS", "schema_name" }
				cmds.SchemaExists = func(schema string) string { return fmt.Sprintf(`SHOW SCHEMAS LIKE '%s'`, schema) }

				cmds.CreateTestTable = func(table string) string {
					return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %[1]s (c1 INT, c2 STRING)", table)
				}
				cmds.ListTables = func(schema string) []lo.Tuple2[string, string] {
					return []lo.Tuple2[string, string]{
						{A: fmt.Sprintf("SHOW TABLES IN %s", schema), B: "tableName"},
					}
				}
				cmds.ListTablesWithPrefix = func(schema, prefix string) []lo.Tuple2[string, string] {
					return []lo.Tuple2[string, string]{
						{A: fmt.Sprintf("SHOW TABLES IN %[1]s LIKE '%[2]s'", schema, prefix+"*"), B: "tableName"},
					}
				}
				cmds.TableExists = func(schema, table string) string {
					return fmt.Sprintf("SHOW TABLES IN %[1]s LIKE '%[2]s'", schema, table)
				}
				cmds.ListColumns = func(schema, table string) (string, string, string) {
					return fmt.Sprintf("DESCRIBE TABLE `%[1]s`.`%[2]s`", schema, table), "col_name", "data_type"
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
