# sqlconnect

Sqlconnect provides a uniform client interface for accessing multiple warehouses:

- bigquery ([configuration](sqlconnect/internal/bigquery/config.go))
- clickhouse ([configuration](sqlconnect/internal/clickhouse/config.go))
- databricks ([configuration](sqlconnect/internal/databricks/config.go))
- mysql ([configuration](sqlconnect/internal/mysql/config.go))
- postgres ([configuration](sqlconnect/internal/postgres/config.go))
- redshift using data API driver ([configuration](sqlconnect/internal/redshift/config.go))
- redshift using postgres driver ([configuration](sqlconnect/internal/postgres/config.go))
- snowflake ([configuration](sqlconnect/internal/snowflake/config.go))
- trino ([configuration](sqlconnect/internal/trino/config.go))

## Installation

```bash
go get github.com/rudderlabs/sqlconnect-go
```

## API

All available `DB` methods can be found [here](sqlconnect/db.go)

## ClickHouse setup

The customer creates the `_rudderstack` database before connection validation.
The driver does not create this database.
Run these statements as an administrator, with your sync user and customer database names:

```sql
CREATE DATABASE IF NOT EXISTS _rudderstack;
GRANT SELECT ON analytics.* TO rudder_retl;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON _rudderstack.* TO rudder_retl;
GRANT SELECT ON system.processes TO rudder_retl;
GRANT SELECT ON system.query_log TO rudder_retl;
```

Sync-log pruning also requires `GRANT ALTER DELETE ON _rudderstack.sync_log TO rudder_retl` after that table exists.
The optional `rudderSchema` credential key overrides `_rudderstack` in rudder-sources.
rudder-sources strips this hidden key before calling the driver and passes the name through `ValidationOptions.WorkingDatabase`.
Account-only validation defaults to `_rudderstack`; the driver rejects `rudderSchema` and `scratchDatabase` account keys.
It is not an account form field.
Use the override in setup, grants, revoke, and teardown statements.
Validation checks database existence, its engine, and the required privileges.

To revoke access and remove working tables, run:

```sql
REVOKE SELECT, INSERT, CREATE TABLE, DROP TABLE ON _rudderstack.* FROM rudder_retl;
DROP DATABASE IF EXISTS _rudderstack SYNC;
```

Revoke `ALTER DELETE` on `_rudderstack.sync_log` first if you granted it.
Drop the database only when no sync uses its tables.

## Usage

**Loading all necessary db drivers**
```go
import _ "github.com/rudderlabs/sqlconnect-go/sqlconnect/config"
```

**Creating a new DB client**
```go
db, err := sqlconnect.NewDB("postgres", []byte(`{
    "host": "postgres.example.com",
    "port": 5432,
    "dbname": "dbname",
    "user": "user",
    "password": "password"

}`))

if err != nil {
    panic(err)
}
```

**Creating a new DB client using legacy mappings for backwards compatibility**
```go
db, err := sqlconnect.NewDB("postgres", []byte(`{
    "host": "postgres.example.com",
    "port": 5432,
    "dbname": "dbname",
    "user": "user",
    "password": "password",
    "legacyMappings": useLegacyMappings

}`))

if err != nil {
    panic(err)
}
```


**Performing admin operations**
```go
{ // schema admin
    exists, err := db.SchemaExists(ctx, sqlconnect.SchemaRef{Name: "schema"})
    if err != nil {
        panic(err)
    }
    if !exists {
        err = db.CreateSchema(ctx, sqlconnect.SchemaRef{Name: "schema"})
        if err != nil {
            panic(err)
        }
    }
}

// table admin
{
    exists, err := db.TableExists(ctx, sqlconnect.NewRelationRef("table", sqlconnect.WithSchema("schema")))
    if err != nil {
        panic(err)
    }
    if !exists {
        err = db.CreateTestTable(ctx, sqlconnect.RelationRef{Schema: "schema", Name: "table"})
        if err != nil {
            panic(err)
        }
    }
}
```

**Using the async query API**
```go
table := sqlconnect.NewRelationRef("table", sqlconnect.WithSchema("schema"))

ch, leave := sqlconnect.QueryJSONAsync(ctx, db, "SELECT * FROM " + db.QuoteTable(table))
defer leave()
for row := range ch {
    if row.Err != nil {
        panic(row.Err)
    }
    _ = row.Value
}
```

## Utilities

**SplitStatements**: Splits a string of SQL statements separated with semicolons into individual statements
```go
import sqlconnectutil "github.com/rudderlabs/sqlconnect-go/sqlconnect/util"

func main() {
    statements := sqlconnectutil.SplitStatements("SELECT * FROM table; SELECT * FROM table;")
}
```
