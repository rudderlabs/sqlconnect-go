package bigquery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/samber/lo"
	"google.golang.org/api/option"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/bigquery/driver"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
)

const (
	DatabaseType = "bigquery"
)

// NewDB creates a new bigquery db client
func NewDB(configJSON json.RawMessage) (*DB, error) {
	var config Config
	err := config.Parse(configJSON)
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(driver.NewConnector(
		config.ProjectID,
		// TODO: switching to WithAuthCredentialsJSON requires auth type handling
		option.WithCredentialsJSON([]byte(config.CredentialsJSON))), // nolint: staticcheck
	)

	return &DB{
		DB: base.NewDB(
			db,
			sshtunnel.NoTunnelCloser,
			base.WithDialect(dialect{base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions())}),
			base.WithColumnTypeMapper(getColumnTypeMapper(config)),
			base.WithJsonRowMapper(getJonRowMapper(config)),
			base.WithSQLCommandsOverride(func(cmds base.SQLCommands) base.SQLCommands {
				cmds.CreateTestTable = func(table base.QuotedIdentifier) string {
					return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %[1]s (c1 INT, c2 STRING)", table)
				}
				cmds.ListTables = func(schema base.UnquotedIdentifier) []lo.Tuple2[string, string] {
					return []lo.Tuple2[string, string]{
						{A: fmt.Sprintf("SELECT table_name FROM `%[1]s`.INFORMATION_SCHEMA.TABLES", schema), B: "table_name"},
					}
				}
				cmds.ListTablesWithPrefix = func(schema base.UnquotedIdentifier, prefix string) []lo.Tuple2[string, string] {
					return []lo.Tuple2[string, string]{
						{A: fmt.Sprintf("SELECT table_name FROM `%[1]s`.INFORMATION_SCHEMA.TABLES WHERE table_name LIKE '%[2]s'", schema, prefix+"%"), B: "table_name"},
					}
				}
				cmds.TableExists = func(schema, table base.UnquotedIdentifier) string {
					return fmt.Sprintf("SELECT table_name FROM `%[1]s`.INFORMATION_SCHEMA.TABLES WHERE table_name = '%[2]s'", schema, base.EscapeSqlString(table))
				}
				cmds.ListColumns = func(catalog, schema, table base.UnquotedIdentifier) (string, string, string) {
					stmt := fmt.Sprintf("SELECT column_name, data_type FROM `%[1]s`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%[2]s'", schema, base.EscapeSqlString(table))
					if catalog != "" {
						stmt += fmt.Sprintf(" AND table_catalog = '%[1]s'", base.EscapeSqlString(catalog))
					}
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

// WithBigqueryClient runs the provided function by providing access to a native bigquery client, the underlying client that is used by the bigquery driver
func (db *DB) WithBigqueryClient(ctx context.Context, f func(*bigquery.Client) error) error {
	sqlconn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = sqlconn.Close() }()
	return sqlconn.Raw(func(driverConn any) error {
		if c, ok := driverConn.(bqclient); ok {
			return f(c.BigqueryClient())
		}
		return fmt.Errorf("invalid driver connection")
	})
}

type bqclient interface {
	BigqueryClient() *bigquery.Client
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
