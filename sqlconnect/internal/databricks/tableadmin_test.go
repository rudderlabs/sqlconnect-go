package databricks

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestTableColumnsStopsBeforeDescribeMetadata(t *testing.T) {
	columns := []sqlconnect.ColumnRef{
		{Name: "id", RawType: "INT"},
		{Name: "# real column", RawType: "STRING"},
		{Name: "# clustering information"},
		{Name: "# col_name", RawType: "data_type"},
	}
	require.Equal(t, columns[:2], tableColumns(columns))

	columns[2] = sqlconnect.ColumnRef{}
	require.Equal(t, columns[:2], tableColumns(columns), "a blank row can also start the metadata section")
}
