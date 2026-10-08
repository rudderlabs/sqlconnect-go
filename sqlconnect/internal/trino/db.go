package trino

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/trinodb/trino-go-client/trino"   //nolint:staticcheck
	_ "github.com/trinodb/trino-go-client/trino" //nolint:staticcheck trino driver

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/util"
)

const (
	DatabaseType = "trino"
)

// NewDB creates a new trino db client
func NewDB(configJSON json.RawMessage) (*DB, error) {
	var config Config
	err := config.Parse(configJSON)
	if err != nil {
		return nil, err
	}
	tunnelCloser, err := sshTunnelling(&config)
	if err != nil {
		return nil, fmt.Errorf("configuring ssh tunnel: %w", err)
	}
	dsn, err := config.ConnectionString()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(DatabaseType, dsn)
	if err != nil {
		return nil, err
	}

	return &DB{
		DB: base.NewDB(
			db,
			tunnelCloser,
			base.WithDialect(newDialect()),
			base.WithColumnTypeMapper(columnTypeMapper),
			base.WithJsonRowMapper(jsonRowMapper),
			base.WithSQLCommandsOverride(func(cmds base.SQLCommands) base.SQLCommands {
				cmds.ListCatalogs = func() (string, string) {
					return "SHOW CATALOGS", "Catalog"
				}
				cmds.ListSchemas = func(catalog base.UnquotedIdentifier) (string, string) {
					if catalog != "" {
						return fmt.Sprintf(`SHOW SCHEMAS FROM "%[1]s"`, catalog), "Schema"
					}
					return "SHOW SCHEMAS", "Schema"
				}
				cmds.SchemaExists = func(catalog, schema base.UnquotedIdentifier) string {
					if catalog != "" {
						return fmt.Sprintf(`SHOW SCHEMAS FROM "%[1]s" LIKE '%[2]s'`, catalog, base.EscapeSqlString(schema))
					}
					return fmt.Sprintf(`SHOW SCHEMAS LIKE '%[1]s'`, base.EscapeSqlString(schema))
				}
				cmds.ListTables = func(catalog, schema base.UnquotedIdentifier, prefix string) []lo.Tuple2[string, string] {
					var qualifier string
					if catalog != "" {
						qualifier = fmt.Sprintf(`"%[1]s"."%[2]s"`, catalog, schema)
					} else {
						qualifier = fmt.Sprintf(`"%[1]s"`, schema)
					}
					if prefix != "" {
						return []lo.Tuple2[string, string]{
							{A: fmt.Sprintf(`SHOW TABLES FROM %[1]s LIKE '%[2]s'`, qualifier, prefix+"%"), B: "tableName"},
						}
					}
					return []lo.Tuple2[string, string]{
						{A: fmt.Sprintf(`SHOW TABLES FROM %[1]s`, qualifier), B: "tableName"},
					}
				}
				cmds.TableExists = func(catalog, schema, table base.UnquotedIdentifier) string {
					if catalog != "" {
						return fmt.Sprintf(`SHOW TABLES FROM "%[1]s"."%[2]s" LIKE '%[3]s'`, catalog, schema, base.EscapeSqlString(table))
					}
					return fmt.Sprintf(`SHOW TABLES FROM "%[1]s" LIKE '%[2]s'`, schema, base.EscapeSqlString(table))
				}
				cmds.TruncateTable = func(table base.QuotedIdentifier) string {
					return fmt.Sprintf(`DELETE FROM %[1]s`, table)
				}
				return cmds
			}),
		),
	}, nil
}

// passing config as a pointer since we need to set [customClientName]. Every
// connection goes through a registered custom client: through the tunnel's
// socks5 proxy when a tunnel is configured, otherwise through a transport whose
// dialer is guarded by the egress policy so the dialled IP is checked.
func sshTunnelling(config *Config) (tunnelCloser func() error, err error) {
	customClientKey := uuid.New().String()
	config.customClientName = customClientKey

	if config.TunnelInfo != nil {
		tunnel, err := sshtunnel.NewSocks5Tunnel(*config.TunnelInfo)
		if err != nil {
			return nil, err
		}
		_ = trino.RegisterCustomClient(customClientKey, &http.Client{
			Transport: sshtunnel.Socks5HTTPTransport(tunnel.Host(), tunnel.Port()),
		})
		return func() error {
			trino.DeregisterCustomClient(customClientKey)
			return tunnel.Close()
		}, nil
	}

	_ = trino.RegisterCustomClient(customClientKey, &http.Client{
		Transport: util.GuardedHTTPTransport(config.SkipHostValidation),
	})
	return func() error {
		trino.DeregisterCustomClient(customClientKey)
		return nil
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

func (db *DB) Ping() error {
	return db.PingContext(context.Background())
}

func (db *DB) PingContext(ctx context.Context) error {
	_, err := db.ExecContext(ctx, "select 1")
	return err
}
