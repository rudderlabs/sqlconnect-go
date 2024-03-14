package driver_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/bigquery/driver"
)

func TestBigqueryDriver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	configJSON, ok := os.LookupEnv("BIGQUERY_TEST_ENVIRONMENT_CREDENTIALS")
	if !ok {
		t.Skip("skipping bigquery driver test due to lack of a test environment")
	}
	var c config
	require.NoError(t, json.Unmarshal([]byte(configJSON), &c))

	t.Run("OpenDB", func(t *testing.T) {
		db := sql.OpenDB(driver.NewConnector(c.ProjectID, option.WithCredentialsJSON([]byte(c.CredentialsJSON))))
		t.Cleanup(func() {
			require.NoError(t, db.Close(), "it should be able to close the database connection")
		})
	})

	q := url.Values{"credentials_json": []string{c.CredentialsJSON}}
	urn := url.URL{Scheme: "bigquery", Host: c.ProjectID, RawQuery: q.Encode()}
	db, err := sql.Open("bigquery", urn.String())
	require.NoError(t, err, "it should be able to open the database connection")
	t.Cleanup(func() {
		require.NoError(t, db.Close(), "it should be able to close the database connection")
	})

	schema := GenerateTestSchema()
	t.Cleanup(func() {
		_, err := db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS `%s` CASCADE", schema))
		require.NoError(t, err, "it should be able to drop the schema")
	})

	t.Run("Ping", func(t *testing.T) {
		require.NoError(t, db.Ping(), "it should be able to ping the database")
		require.NoError(t, db.PingContext(ctx), "it should be able to ping the database using a context")
	})

	t.Run("Transaction unsupported", func(t *testing.T) {
		t.Run("Begin", func(t *testing.T) {
			_, err := db.Begin()
			require.Error(t, err, "it should not be able to begin a transaction")
		})

		t.Run("BeginTx", func(t *testing.T) {
			_, err := db.BeginTx(ctx, nil)
			require.Error(t, err, "it should not be able to begin a transaction")
		})
	})
	t.Run("Exec", func(t *testing.T) {
		_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA `%s`", schema))
		require.NoError(t, err, "it should be able to create a schema")
	})

	t.Run("ExecContext", func(t *testing.T) {
		_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE `%s`.`test_table` (C1 INT, C2 ARRAY<STRING>)", schema))
		require.NoError(t, err, "it should be able to create a table")
	})

	t.Run("prepared statement", func(t *testing.T) {
		t.Run("QueryRow", func(t *testing.T) {
			stmt, err := db.Prepare(fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`test_table`", schema))
			require.NoError(t, err, "it should be able to prepare a statement")
			defer func() {
				require.NoError(t, stmt.Close(), "it should be able to close the prepared statement")
			}()

			var count int
			err = stmt.QueryRow().Scan(&count)
			require.NoError(t, err, "it should be able to execute a prepared statement")
		})

		t.Run("Exec", func(t *testing.T) {
			stmt, err := db.Prepare(fmt.Sprintf("INSERT INTO `%s`.`test_table` (C1) VALUES (?)", schema))
			require.NoError(t, err, "it should be able to prepare a statement")
			defer func() {
				require.NoError(t, stmt.Close(), "it should be able to close the prepared statement")
			}()
			result, err := stmt.Exec(1)
			require.NoError(t, err, "it should be able to execute a prepared statement")

			_, err = result.LastInsertId()
			require.Error(t, err, "last insert id not supported")

			rowsAffected, err := result.RowsAffected()
			require.NoError(t, err, "it should be able to get rows affected")
			require.EqualValues(t, 0, rowsAffected, "rows affected should be 0 (not supported)")
		})

		t.Run("Query", func(t *testing.T) {
			stmt, err := db.Prepare(fmt.Sprintf("SELECT C1 FROM `%s`.`test_table` WHERE C1 = ?", schema))
			require.NoError(t, err, "it should be able to prepare a statement")
			defer func() {
				require.NoError(t, stmt.Close(), "it should be able to close the prepared statement")
			}()
			rows, err := stmt.Query(1)
			require.NoError(t, err, "it should be able to execute a prepared statement")
			defer func() {
				require.NoError(t, rows.Close(), "it should be able to close the rows")
			}()
			require.True(t, rows.Next(), "it should be able to get a row")
			var c1 int
			err = rows.Scan(&c1)
			require.NoError(t, err, "it should be able to scan the row")
			require.EqualValues(t, 1, c1, "it should be able to get the correct value")
			require.False(t, rows.Next(), "it shouldn't have next row")

			require.NoError(t, rows.Err())
		})

		t.Run("Query with named parameters", func(t *testing.T) {
			stmt, err := db.PrepareContext(ctx, fmt.Sprintf("SELECT C1, C2 FROM `%s`.`test_table` WHERE C1 = @c1_value", schema))
			require.NoError(t, err, "it should be able to prepare a statement")
			defer func() {
				require.NoError(t, stmt.Close(), "it should be able to close the prepared statement")
			}()
			rows, err := stmt.QueryContext(ctx, sql.Named("c1_value", 1))
			require.NoError(t, err, "it should be able to execute a prepared statement")
			defer func() {
				require.NoError(t, rows.Close(), "it should be able to close the rows")
			}()

			cols, err := rows.Columns()
			require.NoError(t, err, "it should be able to get the columns")
			require.EqualValues(t, []string{"C1", "C2"}, cols, "it should be able to get the correct columns")

			colTypes, err := rows.ColumnTypes()
			require.NoError(t, err, "it should be able to get the column types")
			require.Len(t, colTypes, 2, "it should be able to get the correct number of column types")
			require.EqualValues(t, "INTEGER", colTypes[0].DatabaseTypeName(), "it should be able to get the correct column type")
			require.EqualValues(t, "ARRAY", colTypes[1].DatabaseTypeName(), "it should be able to get the correct column type")

			require.True(t, rows.Next(), "it should be able to get a row")
			var c1 int
			var c2 any
			err = rows.Scan(&c1, &c2)
			require.NoError(t, err, "it should be able to scan the row")
			require.EqualValues(t, 1, c1, "it should be able to get the correct value")
			require.Nil(t, c2, "it should be able to get the correct value")
			require.False(t, rows.Next(), "it shouldn't have next row")

			require.NoError(t, rows.Err())
		})
	})

	t.Run("query", func(t *testing.T) {
		t.Run("QueryRow", func(t *testing.T) {
			var count int
			err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`test_table`", schema)).Scan(&count)
			require.NoError(t, err, "it should be able to execute a prepared statement")
			require.Equal(t, 1, count, "it should be able to get the correct value")
		})

		t.Run("Exec", func(t *testing.T) {
			result, err := db.Exec(fmt.Sprintf("INSERT INTO `%s`.`test_table` (C1) VALUES (?)", schema), 2)
			require.NoError(t, err, "it should be able to execute a prepared statement")
			rowsAffected, err := result.RowsAffected()
			require.NoError(t, err, "it should be able to get rows affected")
			require.EqualValues(t, 0, rowsAffected, "rows affected should be 0 (not supported)")
		})

		t.Run("Query", func(t *testing.T) {
			rows, err := db.Query(fmt.Sprintf("SELECT C1 FROM `%s`.`test_table` WHERE C1 = ?", schema), 2)
			require.NoError(t, err, "it should be able to execute a prepared statement")
			defer func() {
				require.NoError(t, rows.Close(), "it should be able to close the rows")
			}()
			require.True(t, rows.Next(), "it should be able to get a row")
			var c1 int
			err = rows.Scan(&c1)
			require.NoError(t, err, "it should be able to scan the row")
			require.EqualValues(t, 2, c1, "it should be able to get the correct value")
			require.False(t, rows.Next(), "it shouldn't have next row")

			require.NoError(t, rows.Err())
		})

		t.Run("Query with named parameters", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT C1 FROM `%s`.`test_table` WHERE C1 = @c1_value", schema), sql.Named("c1_value", 2))
			require.NoError(t, err, "it should be able to execute a prepared statement")
			defer func() {
				require.NoError(t, rows.Close(), "it should be able to close the rows")
			}()

			cols, err := rows.Columns()
			require.NoError(t, err, "it should be able to get the columns")
			require.EqualValues(t, []string{"C1"}, cols, "it should be able to get the correct columns")

			colTypes, err := rows.ColumnTypes()
			require.NoError(t, err, "it should be able to get the column types")
			require.Len(t, colTypes, 1, "it should be able to get the correct number of column types")
			require.EqualValues(t, "INTEGER", colTypes[0].DatabaseTypeName(), "it should be able to get the correct column type")

			require.True(t, rows.Next(), "it should be able to get a row")
			var c1 int
			err = rows.Scan(&c1)
			require.NoError(t, err, "it should be able to scan the row")
			require.EqualValues(t, 2, c1, "it should be able to get the correct value")
			require.False(t, rows.Next(), "it shouldn't have next row")

			require.NoError(t, rows.Err())
		})
	})
}

type config struct {
	ProjectID       string `json:"project"`
	CredentialsJSON string `json:"credentials"`
}

func GenerateTestSchema() string {
	return strings.ToLower(fmt.Sprintf("tsqlcon_%s_%d", rand.String(12), time.Now().Unix()))
}
