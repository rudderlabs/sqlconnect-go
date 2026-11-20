package maxcomputequery_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/maxcomputequery"
)

type plainDB struct{ sqlconnect.DB }

type partitionedDB struct {
	sqlconnect.DB
	got sqlconnect.RelationRef
}

func (d *partitionedDB) ListPartitionColumns(_ context.Context, relation sqlconnect.RelationRef) ([]string, error) {
	d.got = relation
	return []string{"ds", "hh"}, nil
}

func TestListPartitionColumns(t *testing.T) {
	ref := sqlconnect.NewSchemaTableRef("s", "t")

	db := &partitionedDB{}
	cols, err := maxcomputequery.ListPartitionColumns(context.Background(), db, ref)
	require.NoError(t, err)
	require.Equal(t, []string{"ds", "hh"}, cols)
	require.Equal(t, ref, db.got)

	_, err = maxcomputequery.ListPartitionColumns(context.Background(), plainDB{}, ref)
	require.ErrorIs(t, err, maxcomputequery.ErrPartitionColumnsUnsupported, "a DB without the lookup says so instead of reporting no partitions")
}
