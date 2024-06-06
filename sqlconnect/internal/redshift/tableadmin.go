package redshift

import (
	"context"
	"fmt"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func (db *DB) ListColumns(ctx context.Context, relation sqlconnect.RelationRef) ([]sqlconnect.ColumnRef, error) {
	// check for non schema binded view
	var count int
	checkSchemaBindedSql := fmt.Sprintf(`select count(*) from (
		SELECT view_definition FROM information_schema.views 
		WHERE table_name = '%s'
		and 
		table_schema = '%s' 
		and 
		view_definition LIKE '%%WITH NO SCHEMA BINDING%%’);`, relation.Name, relation.Schema)
	err := db.QueryRow(checkSchemaBindedSql).Scan(&count)
	if err != nil {
		return nil, fmt.Errorf("error while checking for non schema binded view: %w", err)
	}
	if (count > 0) {
		return db.DB.ListColumnsForSqlQuery(ctx, fmt.Sprintf(`select * from (select * from %s) sq limit 1`, db.QuoteTable(relation)))
	}
	return db.DB.ListColumns(ctx, relation)
}