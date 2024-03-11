package base

import (
	"database/sql"
	"fmt"

	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func NewDB(db *sql.DB, opts ...Option) *DB {
	d := &DB{
		DB:      db,
		Dialect: dialect{},
		columnTypeMapper: func(c ColumnType) string {
			return c.DatabaseTypeName()
		},
		jsonRowMapper: func(databaseTypeName string, value any) any {
			return value
		},
		sqlCommands: SQLCommands{
			CreateSchema: func(schema string) string { return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %[1]s", schema) },
			ListSchemas: func() (string, string) {
				return "SELECT schema_name FROM information_schema.schemata", "schema_name"
			},
			SchemaExists: func(schema string) string {
				return fmt.Sprintf("SELECT schema_name FROM information_schema.schemata where schema_name = '%[1]s'", schema)
			},
			DropSchema: func(schema string) string { return fmt.Sprintf("DROP SCHEMA %[1]s CASCADE", schema) },
			CreateTestTable: func(table string) string {
				return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %[1]s (c1 INT, c2 VARCHAR(255))", table)
			},
			ListTables: func(schema string) []lo.Tuple2[string, string] {
				return []lo.Tuple2[string, string]{
					{A: fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%[1]s'", schema), B: "table_name"},
				}
			},
			ListTablesWithPrefix: func(schema, prefix string) []lo.Tuple2[string, string] {
				return []lo.Tuple2[string, string]{
					{A: fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema='%[1]s' AND table_name LIKE '%[2]s'", schema, prefix+"%"), B: "table_name"},
				}
			},
			TableExists: func(schema, table string) string {
				return fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema='%[1]s' and table_name = '%[2]s'", schema, table)
			},
			ListColumns: func(schema, table string) (string, string, string) {
				return fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%[1]s' AND table_name = '%[2]s'", schema, table), "column_name", "data_type"
			},
			CountTableRows: func(table string) string { return fmt.Sprintf("SELECT COUNT(*) FROM %[1]s", table) },
			DropTable:      func(table string) string { return fmt.Sprintf("DROP TABLE IF EXISTS %[1]s", table) },
			TruncateTable:  func(table string) string { return fmt.Sprintf("TRUNCATE TABLE %[1]s", table) },
			RenameTable: func(schema, oldName, newName string) string {
				return fmt.Sprintf("ALTER TABLE %[1]s.%[2]s RENAME TO %[3]s", schema, oldName, newName)
			},
			MoveTable: func(schema, oldName, newName string) string {
				return fmt.Sprintf("CREATE TABLE %[1]s.%[3]s AS SELECT * FROM %[1]s.%[2]s", schema, oldName, newName)
			},
		},
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

type DB struct {
	*sql.DB
	sqlconnect.Dialect

	columnTypeMapper func(ColumnType) string // map from database type to rudder type
	jsonRowMapper    func(databaseTypeName string, value any) any
	sqlCommands      SQLCommands
}

type ColumnType interface {
	DatabaseTypeName() string
	DecimalSize() (precision, scale int64, ok bool)
}

type colRefTypeAdapter struct {
	sqlconnect.ColumnRef
}

func (c colRefTypeAdapter) DatabaseTypeName() string {
	return c.RawType
}

func (c colRefTypeAdapter) DecimalSize() (precision, scale int64, ok bool) {
	return 0, 0, false
}

// SqlDB returns the underlying *sql.DB
func (db *DB) SqlDB() *sql.DB {
	return db.DB
}

type SQLCommands struct {
	// Provides the SQL command to create a schema
	CreateSchema func(schema string) string
	// Provides the SQL command to list all schemas
	ListSchemas func() (sql, columnName string)
	// Provides the SQL command to check if a schema exists
	SchemaExists func(schema string) string
	// Provides the SQL command to drop a schema
	DropSchema func(schema string) string
	// Provides the SQL command to create a test table
	CreateTestTable func(table string) string
	// Provides the SQL command(s) to list all tables in a schema along with the column name that contains the table name in the result set
	ListTables func(schema string) (sqlAndColumnNamePairs []lo.Tuple2[string, string])
	// Provides the SQL command(s) to list all tables in a schema with a prefix along with the column name that contains the table name in the result set
	ListTablesWithPrefix func(schema, prefix string) []lo.Tuple2[string, string]
	// Provides the SQL command to check if a table exists
	TableExists func(schema, table string) string
	// Provides the SQL command to list all columns in a table along with the column names in the result set that point to the name and type
	ListColumns func(schema, table string) (sql, nameCol, typeCol string)
	// Provides the SQL command to count the rows in a table
	CountTableRows func(table string) string
	// Provides the SQL command to drop a table
	DropTable func(table string) string
	// Provides the SQL command to truncate a table
	TruncateTable func(table string) string
	// Provides the SQL command to rename a table
	RenameTable func(schema, oldName, newName string) string
	// Provides the SQL command to move a table
	MoveTable func(schema, oldName, newName string) string
}
