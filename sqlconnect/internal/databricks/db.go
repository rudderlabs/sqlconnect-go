package databricks

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	databricks "github.com/databricks/databricks-sql-go"
	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
)

const (
	DatabaseType = "databricks"
)

// NewDB creates a new databricks db client
func NewDB(configJson json.RawMessage) (*DB, error) {
	var config Config
	err := config.Parse(configJson)
	if err != nil {
		return nil, err
	}

	opts := newOpts(
		databricks.WithAccessToken(config.Token),
		databricks.WithServerHostname(config.Host),
		databricks.WithPort(config.Port),
		databricks.WithHTTPPath(config.Path),
		databricks.WithInitialNamespace(config.Catalog, config.Schema),
		databricks.WithRetries(
			config.RetryAttempts,
			config.MinRetryWaitTime,
			config.MaxRetryWaitTime,
		),
		databricks.WithTimeout(config.Timeout),
		databricks.WithSessionParams(config.SessionParams),
		databricks.WithUserAgentEntry("Rudderstack"),
	)
	tunnelCloser := sshtunnel.NoTunnelCloser
	if config.TunnelInfo != nil {
		tunnel, err := sshtunnel.NewSocks5Tunnel(*config.TunnelInfo)
		if err != nil {
			return nil, err
		}
		tunnelCloser = tunnel.Close
		// Use a custom http transport in the client to route the connection through the tunnel's socks5 proxy
		opts = append(opts, databricks.WithTransport(sshtunnel.Socks5HTTPTransport(tunnel.Host(), tunnel.Port())))
	}

	connector, err := databricks.NewConnector(opts...)
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)
	db.SetConnMaxIdleTime(config.MaxConnIdleTime)

	if _, err = db.Exec("SELECT * FROM INFORMATION_SCHEMA.COLUMNS LIMIT 1"); err != nil && !strings.Contains(err.Error(), "TABLE_OR_VIEW_NOT_FOUND") {
		return nil, fmt.Errorf("checking if unity catalog is available: %w", err)
	}
	informationSchema := err == nil

	return &DB{
		DB: base.NewDB(
			db,
			tunnelCloser,
			base.WithDialect(dialect{}),
			base.WithColumnTypeMapper(getColumnTypeMapper(config)),
			base.WithJsonRowMapper(getJonRowMapper(config)),
			base.WithSQLCommandsOverride(func(cmds base.SQLCommands) base.SQLCommands {
				cmds.CurrentCatalog = func() string {
					return "SELECT current_catalog()"
				}
				cmds.ListSchemas = func() (string, string) { return "SHOW SCHEMAS", "schema_name" }
				cmds.SchemaExists = func(schema base.UnquotedIdentifier) string {
					return fmt.Sprintf(`SHOW SCHEMAS LIKE '%s'`, base.EscapeSqlString(schema))
				}

				cmds.CreateTestTable = func(table base.QuotedIdentifier) string {
					return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %[1]s (c1 INT, c2 STRING)", table)
				}
				cmds.ListTables = func(schema base.UnquotedIdentifier) []lo.Tuple2[string, string] {
					return []lo.Tuple2[string, string]{
						{A: fmt.Sprintf("SHOW TABLES IN `%s`", base.EscapeSqlString(schema)), B: "tableName"},
					}
				}
				cmds.ListTablesWithPrefix = func(schema base.UnquotedIdentifier, prefix string) []lo.Tuple2[string, string] {
					return []lo.Tuple2[string, string]{
						{A: fmt.Sprintf("SHOW TABLES IN `%[1]s` LIKE '%[2]s'", schema, prefix+"*"), B: "tableName"},
					}
				}
				cmds.TableExists = func(schema, table base.UnquotedIdentifier) string {
					return fmt.Sprintf("SHOW TABLES IN `%[1]s` LIKE '%[2]s'", schema, base.EscapeSqlString(table))
				}
				cmds.ListColumns = func(catalog, schema, table base.UnquotedIdentifier) (string, string, string) {
					if catalog == "" || !informationSchema {
						return fmt.Sprintf("DESCRIBE TABLE `%[1]s`.`%[2]s`", schema, table), "col_name", "data_type"
					}
					stmt := fmt.Sprintf(`SELECT 
											column_name, 
											full_data_type 
										FROM information_schema.columns 
										WHERE table_schema = '%[1]s' 
										AND table_name = '%[2]s'
										AND table_catalog='%[3]s' 
										ORDER BY ORDINAL_POSITION ASC`,
						schema,
						table,
						catalog)
					return stmt, "column_name", "full_data_type"
				}
				cmds.RenameTable = func(schema, oldName, newName base.QuotedIdentifier) string {
					return fmt.Sprintf("ALTER TABLE %[1]s.%[2]s RENAME TO %[1]s.%[3]s", schema, oldName, newName)
				}
				return cmds
			}),
		),
		informationSchema: informationSchema,
	}, nil
}

func init() {
	sqlconnect.RegisterDBFactory(DatabaseType, func(credentialsJSON json.RawMessage) (sqlconnect.DB, error) {
		return NewDB(credentialsJSON)
	})
}

type DB struct {
	*base.DB
	informationSchema bool
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

// This is required because databricks connection option types are unexported...
func newOpts[T any](args ...T) []T {
	var slice []T
	slice = append(slice, args...)
	return slice
}
