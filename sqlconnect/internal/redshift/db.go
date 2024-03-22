package redshift

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/lib/pq" // postgres driver
	"github.com/tidwall/gjson"

	redshiftdriver "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/redshift/driver"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/postgres"
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
	// Use the SDK if the credentials are for the SDK
	if configType := gjson.GetBytes(credentialsJSON, "type").Str; configType == RedshiftDataConfigType {
		db, err = newRedshiftDataDB(credentialsJSON)
	} else {
		db, err = newPostgresDB(credentialsJSON)
	}
	if err != nil {
		return nil, err
	}

	return &DB{
		DB: base.NewDB(
			db,
			base.WithColumnTypeMappings(getColumnTypeMappings(useLegacyMappings)),
			base.WithJsonRowMapper(getJonRowMapper(useLegacyMappings)),
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

func newPostgresDB(credentialsJSON json.RawMessage) (*sql.DB, error) {
	var config PostgresConfig
	err := config.Parse(credentialsJSON)
	if err != nil {
		return nil, err
	}

	return sql.Open(postgres.DatabaseType, config.ConnectionString())
}

func newRedshiftDataDB(credentialsJSON json.RawMessage) (*sql.DB, error) {
	var config Config
	err := config.Parse(credentialsJSON)
	if err != nil {
		return nil, err
	}
	cfg := redshiftdriver.RedshiftConfig{
		ClusterIdentifier: config.ClusterIdentifier,
		Database:          config.Database,
		DbUser:            config.User,
		WorkgroupName:     config.WorkgroupName,
		SecretsARN:        config.SecretsARN,
		Region:            config.Region,
		AccessKeyID:       config.AccessKeyID,
		SecretAccessKey:   config.SecretAccessKey,
		SessionToken:      config.SessionToken,
		Timeout:           config.Timeout,
		MinPolling:        config.MinPolling,
		MaxPolling:        config.MaxPolling,
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
