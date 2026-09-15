# sqlconnect

Sqlconnect provides a uniform client interface for accessing multiple warehouses:

- bigquery ([configuration](sqlconnect/internal/bigquery/config.go))
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

**Postgres session timezone**

The Postgres client inherits the session timezone from lib/pq/Postgres startup defaults, including server, database, user, and deployment-level settings. `sqlconnect-go` only adds connection fields such as host, port, database, user, password, and `sslmode`; it does not add a `TimeZone=UTC` startup parameter or execute `SET TIME ZONE` after connecting.

For rETL rolling-window queries, this means comparisons against `timestamptz` columns keep the same session-timezone dependence for anchored and rolling clock expressions, but rolling windows on `timestamp` (without time zone) columns can shift by the session offset if the actual connector session is not UTC. Verify UTC in the RudderStack rETL Postgres execution environment with `SHOW TIME ZONE` or `SELECT current_setting('TimeZone')` when UTC semantics are required.

This repository does not verify the production RudderStack rETL connector session timezone. Until an observed `SHOW TIME ZONE` or `current_setting('TimeZone')` result from that execution environment is recorded, UTC remains an external deployment, database, or user-level assumption rather than a guarantee provided by `sqlconnect-go`.

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
