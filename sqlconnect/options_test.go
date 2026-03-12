package sqlconnect

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOptions(t *testing.T) {
	o := NewOptions(
		WithSchema("schema"),
		WithCatalog("catalog"),
		WithRelationType(TableRelation),
		WithPrefix("prefix"),
	)
	require.Equal(t, "schema", o.Schema)
	require.Equal(t, "catalog", o.Catalog)
	require.Equal(t, TableRelation, o.Type)
	require.Equal(t, "prefix", o.Prefix)
}

func TestNewTableListOptions(t *testing.T) {
	t.Run("valid with catalog and prefix", func(t *testing.T) {
		opts, err := NewTableListOptions(WithCatalog("catalog"), WithPrefix("prefix"))
		require.NoError(t, err)
		require.Equal(t, "catalog", opts.Catalog)
		require.Equal(t, "prefix", opts.Prefix)
	})

	t.Run("valid with no options", func(t *testing.T) {
		opts, err := NewTableListOptions()
		require.NoError(t, err)
		require.Empty(t, opts.Catalog)
		require.Empty(t, opts.Prefix)
	})

	t.Run("rejects schema", func(t *testing.T) {
		_, err := NewTableListOptions(WithSchema("schema"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema is not supported for table listing")
	})

	t.Run("rejects type", func(t *testing.T) {
		_, err := NewTableListOptions(WithRelationType(TableRelation))
		require.Error(t, err)
		require.Contains(t, err.Error(), "type is not supported for table listing")
	})
}

func TestNewFilterOptions(t *testing.T) {
	t.Run("valid with catalog", func(t *testing.T) {
		opts, err := NewFilterOptions(WithCatalog("catalog"))
		require.NoError(t, err)
		require.Equal(t, "catalog", opts.Catalog)
	})

	t.Run("valid with no options", func(t *testing.T) {
		opts, err := NewFilterOptions()
		require.NoError(t, err)
		require.Empty(t, opts.Catalog)
	})

	t.Run("rejects schema", func(t *testing.T) {
		_, err := NewFilterOptions(WithSchema("schema"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema is not supported for filtering")
	})

	t.Run("rejects prefix", func(t *testing.T) {
		_, err := NewFilterOptions(WithPrefix("prefix"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "prefix is not supported for filtering")
	})

	t.Run("rejects type", func(t *testing.T) {
		_, err := NewFilterOptions(WithRelationType(ViewRelation))
		require.Error(t, err)
		require.Contains(t, err.Error(), "type is not supported for filtering")
	})
}
