package base

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func NewDB(db *sql.DB, tunnelCloser func() error, opts ...Option) *DB {
	d := &DB{
		DB:           db,
		Dialect:      Dialect{},
		tunnelCloser: tunnelCloser,
		columnTypeMapper: func(c ColumnType) string {
			return c.DatabaseTypeName()
		},
		jsonRowMapper: func(databaseTypeName string, value any) any {
			return value
		},
		sqlCommands: SQLCommands{
			CurrentCatalog: func() string {
				return "SELECT current_catalog"
			},
			ListCatalogs: func() (string, string) {
				// Filter template databases (template0, template1) but include the postgres database
				// datistemplate = false excludes template databases
				return "SELECT datname FROM pg_database WHERE datistemplate = false", "datname"
			},
			CreateSchema: func(schema QuotedIdentifier) string {
				return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %[1]s", schema)
			},
			ListSchemas: func(catalog UnquotedIdentifier) (string, string) {
				stmt := "SELECT schema_name FROM information_schema.schemata"
				if catalog != "" {
					stmt += fmt.Sprintf(" WHERE catalog_name = '%[1]s'", EscapeSqlString(catalog))
				}
				return stmt, "schema_name"
			},
			SchemaExists: func(schema, catalog UnquotedIdentifier) string {
				stmt := fmt.Sprintf("SELECT schema_name FROM information_schema.schemata where schema_name = '%[1]s'", EscapeSqlString(schema))
				if catalog != "" {
					stmt += fmt.Sprintf(" AND catalog_name = '%[1]s'", EscapeSqlString(catalog))
				}
				return stmt
			},
			DropSchema: func(schema QuotedIdentifier) string { return fmt.Sprintf("DROP SCHEMA %[1]s CASCADE", schema) },
			CreateTestTable: func(table QuotedIdentifier) string {
				return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %[1]s (c1 INT, c2 VARCHAR(255))", table)
			},
			ListTables: func(schema, catalog UnquotedIdentifier, prefix string) []lo.Tuple2[string, string] {
				stmt := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%[1]s'", EscapeSqlString(schema))
				if catalog != "" {
					stmt += fmt.Sprintf(" AND table_catalog = '%[1]s'", EscapeSqlString(catalog))
				}
				if prefix != "" {
					stmt += fmt.Sprintf(" AND table_name LIKE '%[1]s'", prefix+"%%")
				}
				return []lo.Tuple2[string, string]{
					{A: stmt, B: "table_name"},
				}
			},
			TableExists: func(schema, table, catalog UnquotedIdentifier) string {
				stmt := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema='%[1]s' and table_name = '%[2]s'", EscapeSqlString(schema), EscapeSqlString(table))
				if catalog != "" {
					stmt += fmt.Sprintf(" AND table_catalog = '%[1]s'", EscapeSqlString(catalog))
				}
				return stmt
			},
			ListColumns: func(catalog, schema, table UnquotedIdentifier) (string, string, string) {
				stmt := fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%[1]s' AND table_name = '%[2]s'", EscapeSqlString(schema), EscapeSqlString(table))
				if catalog != "" {
					stmt += fmt.Sprintf(" AND table_catalog = '%[1]s'", EscapeSqlString(catalog))
				}
				return stmt + " ORDER BY ordinal_position ASC", "column_name", "data_type"
			},
			CountTableRows: func(table QuotedIdentifier) string { return fmt.Sprintf("SELECT COUNT(*) FROM %[1]s", table) },
			DropTable:      func(table QuotedIdentifier) string { return fmt.Sprintf("DROP TABLE IF EXISTS %[1]s", table) },
			TruncateTable:  func(table QuotedIdentifier) string { return fmt.Sprintf("TRUNCATE TABLE %[1]s", table) },
			RenameTable: func(schema, oldName, newName QuotedIdentifier) string {
				return fmt.Sprintf("ALTER TABLE %[1]s.%[2]s RENAME TO %[3]s", schema, oldName, newName)
			},
			MoveTable: func(schema, oldName, newName QuotedIdentifier) string {
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
	tunnelCloser func() error // closer for the ssh tunnel to be called on close

	columnTypeMapper func(ColumnType) string // map from database type to rudder type
	jsonRowMapper    func(databaseTypeName string, value any) any
	sqlCommands      SQLCommands
}

// Close closes the db and the tunnel
func (d *DB) Close() error {
	return errors.Join(
		d.DB.Close(),     // first close the db
		d.tunnelCloser(), // then close the tunnel
	)
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

type (
	QuotedIdentifier   string // A quoted identifier is a string that is quoted, e.g. "my_table"
	UnquotedIdentifier string // An unquoted identifier is a string that is not quoted, e.g. my_table
	SQLCommands        struct {
		// Provides the SQL command to get the current catalog
		CurrentCatalog func() string
		// Provides the SQL command to list all catalogs/databases
		ListCatalogs func() (sql, columnName string)
		// Provides the SQL command to create a schema
		CreateSchema func(schema QuotedIdentifier) string
		// Provides the SQL command to list schemas, optionally filtered by catalog
		ListSchemas func(catalog UnquotedIdentifier) (sql, columnName string)
		// Provides the SQL command to check if a schema exists, optionally within a catalog
		SchemaExists func(schema, catalog UnquotedIdentifier) string
		// Provides the SQL command to drop a schema,
		DropSchema func(schema QuotedIdentifier) string
		// Provides the SQL command to create a test table
		CreateTestTable func(table QuotedIdentifier) string
		// Provides the SQL command(s) to list tables in a schema, optionally filtered by catalog and/or prefix
		ListTables func(schema, catalog UnquotedIdentifier, prefix string) (sqlAndColumnNamePairs []lo.Tuple2[string, string])
		// Provides the SQL command to check if a table exists, optionally within a catalog
		TableExists func(schema, table, catalog UnquotedIdentifier) string
		// Provides the SQL command to list all columns in a table along with the column names in the result set that point to the name and type
		ListColumns func(catalog, schema, table UnquotedIdentifier) (sql, nameCol, typeCol string)
		// Provides the SQL command to count the rows in a table
		CountTableRows func(table QuotedIdentifier) string
		// Provides the SQL command to drop a table
		DropTable func(table QuotedIdentifier) string
		// Provides the SQL command to truncate a table
		TruncateTable func(table QuotedIdentifier) string
		// Provides the SQL command to rename a table
		RenameTable func(schema, oldName, newName QuotedIdentifier) string
		// Provides the SQL command to move a table
		MoveTable func(schema, oldName, newName QuotedIdentifier) string
	}
)
