package fabric

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func TestNewDBBuildsAzureADConnector(t *testing.T) {
	db, err := NewDB(json.RawMessage(`{
		"host":"localhost", "database":"warehouse", "tenantId":"tenant",
		"clientId":"client", "clientSecret":"secret", "skipHostValidation":true
	}`))
	require.NoError(t, err)
	require.NotNil(t, db.SqlDB())
	require.NoError(t, db.Close())
}

func TestFabricSQLCommands(t *testing.T) {
	commands := fabricSQLCommands(base.SQLCommands{})
	require.Equal(t, "SELECT DB_NAME()", commands.CurrentCatalog())
	listCatalogs, catalogColumn := commands.ListCatalogs()
	require.Equal(t, "SELECT name FROM sys.databases WHERE name <> 'master'", listCatalogs)
	require.Equal(t, "name", catalogColumn)
	require.Equal(t, "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'sch''ema') EXEC(N'CREATE SCHEMA [sch''ema]')", commands.CreateSchema("[sch'ema]"))
	require.Equal(t, "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'sch'']ema]x') EXEC(N'CREATE SCHEMA [sch'']ema]]x]')", commands.CreateSchema("[sch']ema]]x]"))
	require.Equal(t, "IF OBJECT_ID(N'[schema].[table]', N'U') IS NULL CREATE TABLE [schema].[table] (c1 INT, c2 VARCHAR(255))", commands.CreateTestTable("[schema].[table]"))
	require.Equal(t, "EXEC sp_rename N'[schema].[old]', N'new', N'OBJECT'", commands.RenameTable("[schema]", "[old]", "[new]"))
}

func TestFabricCatalogMetadataCommandsGuardMissingCatalogs(t *testing.T) {
	commands := fabricSQLCommands(base.SQLCommands{})

	listSchemas, schemaColumn := commands.ListSchemas("missing]'catalog")
	require.Equal(t, "schema_name", schemaColumn)
	require.Equal(t, "IF DB_ID(N'missing]''catalog') IS NULL SELECT CAST(NULL AS NVARCHAR(128)) AS schema_name WHERE 1 = 0 ELSE EXEC(N'SELECT SCHEMA_NAME AS schema_name FROM [missing]]''catalog].INFORMATION_SCHEMA.SCHEMATA')", listSchemas)

	require.Equal(t, "IF DB_ID(N'missing') IS NULL SELECT CAST(NULL AS NVARCHAR(128)) AS schema_name WHERE 1 = 0 ELSE EXEC(N'SELECT SCHEMA_NAME AS schema_name FROM [missing].INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ''public''')", commands.SchemaExists("missing", "public"))
	require.Equal(t, "IF DB_ID(N'missing') IS NULL SELECT CAST(NULL AS NVARCHAR(128)) AS table_name WHERE 1 = 0 ELSE EXEC(N'SELECT TABLE_NAME AS table_name FROM [missing].INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''public'' AND TABLE_NAME = ''events''')", commands.TableExists("missing", "public", "events"))

	tables := commands.ListTables("missing", "public", "ev")
	require.Len(t, tables, 1)
	require.Equal(t, "table_name", tables[0].B)
	require.Equal(t, "IF DB_ID(N'missing') IS NULL SELECT CAST(NULL AS NVARCHAR(128)) AS table_name WHERE 1 = 0 ELSE EXEC(N'SELECT TABLE_NAME AS table_name FROM [missing].INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''public'' AND TABLE_NAME LIKE ''ev%''')", tables[0].A)

	listColumns, nameColumn, typeColumn := commands.ListColumns("missing", "public", "events")
	require.Equal(t, "column_name", nameColumn)
	require.Equal(t, "data_type", typeColumn)
	require.Equal(t, "IF DB_ID(N'missing') IS NULL SELECT CAST(NULL AS NVARCHAR(128)) AS column_name, CAST(NULL AS NVARCHAR(128)) AS data_type WHERE 1 = 0 ELSE EXEC(N'SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type FROM [missing].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''public'' AND TABLE_NAME = ''events'' ORDER BY ORDINAL_POSITION ASC')", listColumns)
}
