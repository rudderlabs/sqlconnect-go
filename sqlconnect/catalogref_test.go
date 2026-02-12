package sqlconnect_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestCatalogRef(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		c := sqlconnect.CatalogRef{Name: "my_catalog"}
		require.Equal(t, "my_catalog", c.String(), "catalog name should be returned")
	})

	t.Run("json marshaling", func(t *testing.T) {
		c := sqlconnect.CatalogRef{Name: "test_db"}
		data, err := json.Marshal(c)
		require.NoError(t, err)
		require.JSONEq(t, `{"name":"test_db"}`, string(data))
	})

	t.Run("json unmarshaling", func(t *testing.T) {
		var c sqlconnect.CatalogRef
		err := json.Unmarshal([]byte(`{"name":"test_db"}`), &c)
		require.NoError(t, err)
		require.Equal(t, "test_db", c.Name)
	})
}
