package sqlconnect_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestSchemaRef(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		s := sqlconnect.SchemaRef{Name: "schema"}
		require.Equal(t, "schema", s.String(), "schema name should be returned")
	})
}
