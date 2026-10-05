package fabric

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/microsoft/go-mssqldb/azuread"
	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/sshtunnel"
)

const DatabaseType = "fabric"

// NewDB creates a Microsoft Fabric SQL endpoint client.
func NewDB(configJSON json.RawMessage) (*DB, error) {
	var config Config
	if err := config.Parse(configJSON); err != nil {
		return nil, err
	}
	if config.FabricWorkspaceID != "" {
		ctx := context.Background()
		if config.Timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, config.Timeout)
			defer cancel()
		}
		if err := defaultBootstrapper.bootstrap(ctx, config); err != nil {
			return nil, err
		}
	}

	connector, err := azuread.NewConnector(config.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("creating Entra SQL connector: %w", err)
	}
	db := sql.OpenDB(connector)

	return &DB{DB: base.NewDB(
		db,
		sshtunnel.NoTunnelCloser,
		base.WithDialect(newDialect()),
		base.WithColumnTypeMapper(columnTypeMapper),
		base.WithJsonRowMapper(jsonRowMapper),
		base.WithSQLCommandsOverride(fabricSQLCommands),
	)}, nil
}

func init() {
	sqlconnect.RegisterDBFactory(DatabaseType, func(credentialsJSON json.RawMessage) (sqlconnect.DB, error) {
		return NewDB(credentialsJSON)
	})
}

type DB struct {
	*base.DB
}

func fabricSQLCommands(cmds base.SQLCommands) base.SQLCommands {
	cmds.CurrentCatalog = func() string { return "SELECT DB_NAME()" }
	cmds.ListCatalogs = func() (string, string) { return "SELECT name FROM sys.databases", "name" }
	cmds.CreateSchema = func(schema base.QuotedIdentifier) string {
		quoted := base.EscapeSqlString(base.UnquotedIdentifier(schema))
		return fmt.Sprintf("IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'%[1]s') EXEC(N'CREATE SCHEMA %[2]s')", base.EscapeSqlString(base.UnquotedIdentifier(unquoteBracketIdentifier(string(schema)))), quoted)
	}
	cmds.ListSchemas = func(catalog base.UnquotedIdentifier) (string, string) {
		stmt := "SELECT schema_name FROM information_schema.schemata"
		return catalogMetadataQuery(catalog, stmt, "schema_name"), "schema_name"
	}
	cmds.SchemaExists = func(catalog, schema base.UnquotedIdentifier) string {
		stmt := fmt.Sprintf("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s'", base.EscapeSqlString(schema))
		return catalogMetadataQuery(catalog, stmt, "schema_name")
	}
	cmds.DropSchema = func(schema base.QuotedIdentifier) string { return fmt.Sprintf("DROP SCHEMA %s", schema) }
	cmds.CreateTestTable = func(table base.QuotedIdentifier) string {
		literal := base.EscapeSqlString(base.UnquotedIdentifier(table))
		return fmt.Sprintf("IF OBJECT_ID(N'%[1]s', N'U') IS NULL CREATE TABLE %[2]s (c1 INT, c2 VARCHAR(255))", literal, table)
	}
	cmds.ListTables = func(catalog, schema base.UnquotedIdentifier, prefix string) []lo.Tuple2[string, string] {
		stmt := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", base.EscapeSqlString(schema))
		if prefix != "" {
			stmt += fmt.Sprintf(" AND table_name LIKE '%s'", base.EscapeSqlString(base.UnquotedIdentifier(prefix+"%")))
		}
		return []lo.Tuple2[string, string]{{A: catalogMetadataQuery(catalog, stmt, "table_name"), B: "table_name"}}
	}
	cmds.TableExists = func(catalog, schema, table base.UnquotedIdentifier) string {
		stmt := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'", base.EscapeSqlString(schema), base.EscapeSqlString(table))
		return catalogMetadataQuery(catalog, stmt, "table_name")
	}
	cmds.ListColumns = func(catalog, schema, table base.UnquotedIdentifier) (string, string, string) {
		stmt := fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position ASC", base.EscapeSqlString(schema), base.EscapeSqlString(table))
		return catalogMetadataQuery(catalog, stmt, "column_name", "data_type"), "column_name", "data_type"
	}
	cmds.RenameTable = func(schema, oldName, newName base.QuotedIdentifier) string {
		oldTable := base.EscapeSqlString(base.UnquotedIdentifier(string(schema) + "." + string(oldName)))
		newTable := base.EscapeSqlString(base.UnquotedIdentifier(unquoteBracketIdentifier(string(newName))))
		return fmt.Sprintf("EXEC sp_rename N'%s', N'%s', N'OBJECT'", oldTable, newTable)
	}
	cmds.MoveTable = func(schema, oldName, newName base.QuotedIdentifier) string {
		return fmt.Sprintf("CREATE TABLE %[1]s.%[3]s AS SELECT * FROM %[1]s.%[2]s", schema, oldName, newName)
	}
	cmds.CreateTableFromQuery = func(table base.QuotedIdentifier, query string) string {
		return fmt.Sprintf("CREATE TABLE %s AS %s", table, query)
	}
	return cmds
}

func catalogMetadataQuery(catalog base.UnquotedIdentifier, stmt string, columns ...string) string {
	if catalog == "" {
		return stmt
	}

	quotedCatalog := "[" + escapeBracketIdentifier(string(catalog)) + "]"
	qualifiedStmt := strings.Replace(stmt, "information_schema.", quotedCatalog+".information_schema.", 1)
	emptyColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		emptyColumns = append(emptyColumns, fmt.Sprintf("CAST(NULL AS NVARCHAR(128)) AS %s", column))
	}
	return fmt.Sprintf(
		"IF DB_ID(N'%s') IS NULL SELECT %s WHERE 1 = 0 ELSE EXEC(N'%s')",
		base.EscapeSqlString(catalog),
		strings.Join(emptyColumns, ", "),
		base.EscapeSqlString(base.UnquotedIdentifier(qualifiedStmt)),
	)
}

func quoteFabricIdentifier(value string) string { return "[" + escapeBracketIdentifier(value) + "]" }

func escapeBracketIdentifier(value string) string { return strings.ReplaceAll(value, "]", "]]") }

func unquoteBracketIdentifier(value string) string {
	if len(value) >= 2 && value[0] == '[' && value[len(value)-1] == ']' {
		return strings.ReplaceAll(value[1:len(value)-1], "]]", "]")
	}
	return value
}
