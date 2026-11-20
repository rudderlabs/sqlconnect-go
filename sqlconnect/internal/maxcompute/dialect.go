package maxcompute

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcsql"
)

// dialect is the MaxCompute dialect. project, when set, is the catalog of
// every relation that names none: MaxCompute reads a two-part name as
// schema.table, so a table with a schema is always quoted in three parts.
type dialect struct {
	*base.GoquDialect
	project string
}

func newDialect(project string) dialect {
	return dialect{GoquDialect: base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions()), project: project}
}

// QuoteIdentifier quotes name with backticks. A name holding a backtick, for
// which MaxCompute has no escape, becomes an identifier the driver refuses.
func (d dialect) QuoteIdentifier(name string) string { return mcsql.QuoteIdentifier(name) }

// QuoteTable quotes `project`.`schema`.`table`. A ref without a catalog gets
// the configured project; a ref without a schema is quoted as is. A part
// holding a dot or a backtick becomes an identifier the driver refuses.
func (d dialect) QuoteTable(table sqlconnect.RelationRef) string {
	catalog := table.Catalog
	if catalog == "" && table.Schema != "" {
		catalog = d.project
	}
	if table.Schema == "" {
		catalog = ""
	}
	return mcsql.QuoteRelation(catalog, table.Schema, table.Name)
}

// FormatTableName lower-cases name: MaxCompute folds every name.
func (d dialect) FormatTableName(name string) string { return mcsql.Normalise(name) }

// NormaliseIdentifier lower-cases the whole identifier: MaxCompute folds
// names even inside backticks.
func (d dialect) NormaliseIdentifier(identifier string) string { return mcsql.Normalise(identifier) }

var errBacktickInName = errors.New("maxcompute: a name cannot hold a backtick")

// ParseRelationRef parses table, schema.table or project.schema.table, each
// part bare or backtick-quoted, and lower-cases every part.
func (d dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	ref, err := base.ParseRelationRef(identifier, '`', strings.ToLower)
	if err != nil {
		return ref, err
	}
	for _, p := range []string{ref.Catalog, ref.Schema, ref.Name} {
		if strings.Contains(p, "`") {
			return sqlconnect.RelationRef{}, errBacktickInName
		}
	}
	ref.Catalog, ref.Schema, ref.Name = mcsql.Normalise(ref.Catalog), mcsql.Normalise(ref.Schema), mcsql.Normalise(ref.Name)
	ref.Type = sqlconnect.TableRelation
	return ref, nil
}

// QueryCondition renders a time.Time argument as an epoch instant, exact in
// every project time zone.
func (d dialect) QueryCondition(identifier, operator string, args ...any) (sqlconnect.Expression, error) {
	args = lo.Map(args, func(a any, _ int) any {
		if t, ok := a.(time.Time); ok {
			return timeLiteral(t)
		}
		return a
	})
	return d.GoquDialect.QueryCondition(identifier, operator, args...)
}

// dialectOptions are the options of the standalone dialect.
type dialectOptions struct {
	Project string `json:"project"`
}

func init() {
	sqlconnect.RegisterDialectFactory(DatabaseType, func(optionsJSON json.RawMessage) (sqlconnect.Dialect, error) {
		var o dialectOptions
		if len(optionsJSON) > 0 {
			if err := json.Unmarshal(optionsJSON, &o); err != nil {
				return nil, errors.New("maxcompute: invalid dialect options")
			}
		}
		return newDialect(o.Project), nil
	})
}
