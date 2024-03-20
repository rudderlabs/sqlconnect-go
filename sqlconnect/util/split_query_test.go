package util_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/util"
)

func TestSplitStatements(t *testing.T) {
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
