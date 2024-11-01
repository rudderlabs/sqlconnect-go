package redshift

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaDoesNotExistRegex(t *testing.T) {
	require.True(t, schemaDoesNotExistRegex.MatchString(`ERROR: schema "tsqlcon_zvtquuiqulpz_1728651180" does not exist`))
}
