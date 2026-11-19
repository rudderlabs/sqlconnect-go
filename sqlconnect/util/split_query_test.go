package util_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/util"
)

func TestSplitStatements(t *testing.T) {
	t.Run("multi-byte text inside string literals", func(t *testing.T) {
		query := "INSERT INTO t VALUES ('مرحبا', 'it''s ; 中文');\nSELECT 'é''x' AS a;\nSELECT 'ends with مرحبا'"
		expected := []string{"INSERT INTO t VALUES ('مرحبا', 'it''s ; 中文')", "SELECT 'é''x' AS a", "SELECT 'ends with مرحبا'"}
		require.Equal(t, expected, util.SplitStatements(query))

		// The closing quote sits at a byte offset past the rune count.
		query = "SELECT 'مرحبا مرحبا مرحبا مرحبا'; SELECT 1"
		require.Equal(t, []string{"SELECT 'مرحبا مرحبا مرحبا مرحبا'", "SELECT 1"}, util.SplitStatements(query))
	})

	t.Run("single statement", func(t *testing.T) {
		query := "SELECT * FROM table"
		expected := []string{"SELECT * FROM table"}
		actual := util.SplitStatements(query)
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("single statement with semicolon", func(t *testing.T) {
		query := "SELECT * FROM table;"
		expected := []string{"SELECT * FROM table"}
		actual := util.SplitStatements(query)
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("multiple statements", func(t *testing.T) {
		query := `
		SELECT * FROM table1;
		SELECT * FROM table2;
		 
		`
		expected := []string{"SELECT * FROM table1", "SELECT * FROM table2"}
		actual := util.SplitStatements(query)
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("multiple statements with simple comments", func(t *testing.T) {
		query := `
		SELECT * FROM table1; -- this is an inline comment
		-- this is another comment on its own line
		SELECT * FROM table2
		`
		expected := []string{"SELECT * FROM table1", "SELECT * FROM table2"}
		actual := util.SplitStatements(query)
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("multiple statements with bracketed comments", func(t *testing.T) {
		query := `
		SELECT * FROM table1; /* this is a bracketed comment
		that spans multiple lines */
		/* this is another bracketed comment */
		SELECT * FROM table2;
		`
		expected := []string{"SELECT * FROM table1", "SELECT * FROM table2"}
		actual := util.SplitStatements(query)
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("multiple statements with both types of comments", func(t *testing.T) {
		query := `
		SELECT * FROM table1; -- this is an inline comment
		/* this is a bracketed comment
		that spans multiple lines */
		-- this is another inline comment
		SELECT * FROM table2;
		/* this is another bracketed 
		comment */
		`
		expected := []string{"SELECT * FROM table1", "SELECT * FROM table2"}
		actual := util.SplitStatements(query)
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("multiple statements with semicolon inside comments", func(t *testing.T) {
		query := `
		SELECT * FROM table1; -- this is an inline comment;
		/* this is a bracketed comment;
		that spans multiple lines */
		-- this is another inline; comment;
		SELECT * FROM table2;
		/* this is another bracketed 
		comment; */
		`
		expected := []string{"SELECT * FROM table1", "SELECT * FROM table2"}
		actual := util.SplitStatements(query)
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("multiple mulitline statements with semicolon in sql string", func(t *testing.T) {
		query := `
		SELECT * 
			FROM table1 
		WHERE value='some;value';
		SELECT 
			* 
		FROM table2 
		  WHERE value='another '' ; value;';
		SELECT * FROM table3 WHERE value='' AND value1='some' ;
		`
		expected := []string{
			"SELECT * \n\t\t\tFROM table1 \n\t\tWHERE value='some;value'",
			"SELECT \n\t\t\t* \n\t\tFROM table2 \n\t\t  WHERE value='another '' ; value;'",
			"SELECT * FROM table3 WHERE value='' AND value1='some'",
		}
		actual := util.SplitStatements(query)
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("single statement with simple comment char sequence in sql string", func(t *testing.T) {
		query := `SELECT * FROM table1 WHERE value='some --value'`

		expected := []string{
			"SELECT * FROM table1 WHERE value='some --value'",
		}
		actual := util.SplitStatements(query)
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("single statement with bracketed comment char sequence in sql string", func(t *testing.T) {
		query := `SELECT * FROM table1 WHERE value='some /* value */'`

		expected := []string{
			"SELECT * FROM table1 WHERE value='some /* value */'",
		}
		actual := util.SplitStatements(query)
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("single statement with all kinds of impediments in sql string", func(t *testing.T) {
		query := `SELECT * FROM table1 WHERE value='''some /* value */ -- comment'''`

		expected := []string{
			"SELECT * FROM table1 WHERE value='''some /* value */ -- comment'''",
		}
		actual := util.SplitStatements(query)
		require.ElementsMatch(t, expected, actual)
	})
}
