package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"

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
			base.WithGoquDialect(base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions())),
			base.WithColumnTypeMapper(getColumnTypeMapper(config)),
			base.WithJsonRowMapper(getJonRowMapper(config)),
			base.WithSQLCommandsOverride(func(cmds base.SQLCommands) base.SQLCommands {
				// information_schema.columns reports data_type = 'ARRAY' without the
				// element type; pg_catalog.format_type yields text[], integer[], etc.
				cmds.ListColumns = func(catalog, schema, table base.UnquotedIdentifier) (string, string, string) {
					catalogClause := ""
					if catalog != "" {
						catalogClause = fmt.Sprintf(" AND current_database() = '%s'", base.EscapeSqlString(catalog))
					}
					stmt := fmt.Sprintf(
						"SELECT a.attname AS column_name, pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type "+
							"FROM pg_catalog.pg_attribute a "+
							"JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "+
							"JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "+
							"WHERE n.nspname = '%[1]s' AND c.relname = '%[2]s' "+
							"AND a.attnum > 0 AND NOT a.attisdropped%[3]s "+
							"ORDER BY a.attnum",
						base.EscapeSqlString(schema), base.EscapeSqlString(table), catalogClause)
					return stmt, "column_name", "data_type"
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
