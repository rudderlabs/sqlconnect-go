package redshift

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq" // postgres driver
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	redshiftdriver "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/redshift/driver"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/postgres"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
)

const (
	DatabaseType = "redshift"
)

// NewDB creates a new redshift db client
func NewDB(credentialsJSON json.RawMessage) (*DB, error) {
	var (
		db  *sql.DB
		err error
	)
	useLegacyMappings := gjson.GetBytes(credentialsJSON, "useLegacyMappings").Bool()
	tunnelCloser := sshtunnel.NoTunnelCloser
	// Use the SDK if the credentials are for the SDK
	if configType := gjson.GetBytes(credentialsJSON, "type").Str; configType == RedshiftDataConfigType {
		db, err = newRedshiftDataDB(credentialsJSON)
	} else {
		db, tunnelCloser, err = newPostgresDB(credentialsJSON)
	}
	if err != nil {
		return nil, err
	}
	var caseSensitive string

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = db.QueryRowContext(ctx, "show enable_case_sensitive_identifier").Scan(&caseSensitive)
	}()

	return &DB{
		DB: base.NewDB(
			db,
			tunnelCloser,
			base.WithDialect(dialect{GoquDialect: base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions()), caseSensitive: caseSensitive == "on"}),
			base.WithColumnTypeMappings(getColumnTypeMappings(useLegacyMappings)),
			base.WithJsonRowMapper(getJonRowMapper(useLegacyMappings)),
			base.WithSQLCommandsOverride(func(cmds base.SQLCommands) base.SQLCommands {
				cmds.CurrentCatalog = func() string {
					return "SELECT current_database()"
				}
				cmds.ListSchemas = func() (string, string) {
					return "SELECT schema_name FROM svv_all_schemas", "schema_name"
				}
				cmds.SchemaExists = func(schema base.UnquotedIdentifier) string {
					return fmt.Sprintf("SELECT schema_name FROM svv_all_schemas WHERE schema_name = '%[1]s'", base.EscapeSqlString(schema))
				}
				cmds.ListTables = func(schema base.UnquotedIdentifier) (sqlAndColumnNamePairs []lo.Tuple2[string, string]) {
					return []lo.Tuple2[string, string]{
						{A: fmt.Sprintf("SELECT table_name FROM svv_all_tables WHERE schema_name = '%[1]s'", base.EscapeSqlString(schema)), B: "table_name"},
					}
				}
				cmds.ListTablesWithPrefix = func(schema base.UnquotedIdentifier, prefix string) []lo.Tuple2[string, string] {
					return []lo.Tuple2[string, string]{
						{A: fmt.Sprintf("SELECT table_name FROM svv_all_tables WHERE schema_name='%[1]s' AND table_name LIKE '%[2]s'", base.EscapeSqlString(schema), prefix+"%"), B: "table_name"},
					}
				}
				cmds.TableExists = func(schema, table base.UnquotedIdentifier) string {
					return fmt.Sprintf("SELECT table_name FROM svv_all_tables WHERE schema_name='%[1]s' and table_name = '%[2]s'", base.EscapeSqlString(schema), base.EscapeSqlString(table))
				}
				cmds.ListColumns = func(catalog, schema, table base.UnquotedIdentifier) (string, string, string) {
					stmt := fmt.Sprintf("SELECT column_name, data_type FROM SVV_ALL_COLUMNS WHERE schema_name = '%[1]s' AND table_name = '%[2]s'", base.EscapeSqlString(schema), base.EscapeSqlString(table))
					if catalog != "" {
						stmt += fmt.Sprintf(" AND database_name = '%[1]s'", base.EscapeSqlString(catalog))
					}
					return stmt + " ORDER BY ordinal_position ASC", "column_name", "data_type"
				}
				return cmds
			}),
		),
	}, nil
}

func newPostgresDB(credentialsJSON json.RawMessage) (*sql.DB, func() error, error) {
	var config PostgresConfig
	err := config.Parse(credentialsJSON)
	if err != nil {
		return nil, nil, err
	}
	tunnelCloser := sshtunnel.NoTunnelCloser
	if config.TunnelInfo != nil {
		tunnel, err := sshtunnel.NewTcpTunnel(*config.TunnelInfo, config.Host, config.Port)
		if err != nil {
			return nil, nil, err
		}
		tunnelCloser = tunnel.Close
		// Update the remote host and port to the tunnel's host and port
		config.Host = tunnel.Host()
		config.Port = tunnel.Port()
	}

	db, err := sql.Open(postgres.DatabaseType, config.ConnectionString())
	if err != nil {
		return nil, nil, err
	}
	return db, tunnelCloser, nil
}

func newRedshiftDataDB(credentialsJSON json.RawMessage) (*sql.DB, error) {
	var config Config
	err := config.Parse(credentialsJSON)
	if err != nil {
		return nil, err
	}
	cfg := redshiftdriver.RedshiftConfig{
		ClusterIdentifier:   config.ClusterIdentifier,
		Database:            config.Database,
		DbUser:              config.User,
		WorkgroupName:       config.WorkgroupName,
		SecretsARN:          config.SecretsARN,
		Region:              config.Region,
		AccessKeyID:         config.AccessKeyID,
		SharedConfigProfile: config.SharedConfigProfile,
		SecretAccessKey:     config.SecretAccessKey,
		SessionToken:        config.SessionToken,
		RoleARN:             config.RoleARN,
		ExternalID:          config.ExternalID,
		RoleARNExpiry:       config.RoleARNExpiry,
		Timeout:             config.Timeout,
		MinPolling:          config.MinPolling,
		MaxPolling:          config.MaxPolling,
		RetryMaxAttempts:    config.RetryMaxAttempts,
	}
	connector := redshiftdriver.NewRedshiftConnector(cfg)

	return sql.OpenDB(connector), nil
}

func init() {
	sqlconnect.RegisterDBFactory(DatabaseType, func(credentialsJSON json.RawMessage) (sqlconnect.DB, error) {
		return NewDB(credentialsJSON)
	})
}

type DB struct {
	*base.DB
}

func getColumnTypeMappings(useLegacyMappings bool) map[string]string {
	if useLegacyMappings {
		return legacyColumnTypeMappings
	}
	return columnTypeMappings
}

func getJonRowMapper(useLegacyMappings bool) func(databaseTypeName string, value any) any {
	if useLegacyMappings {
		return legacyJsonRowMapper
	}
	return jsonRowMapper
}
