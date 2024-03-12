package sqlconnect

import (
	"encoding/json"
	"fmt"
)

func NewRelationRef(name string, options ...Option) RelationRef {
	var o RelationRefOption
	for _, option := range options {
		option(&o)
	}

	relationType := TableRelation
	if o.Type != "" {
		relationType = o.Type
	}
	return RelationRef{
		Name:    name,
		Schema:  o.Schema,
		Catalog: o.Catalog,
		Type:    relationType,
	}
}

// NewSchemaTableRef creates a new RelationRef with a schema and a table
func NewSchemaTableRef(schema, table string) RelationRef {
	return NewRelationRef(table, WithSchema(schema))
}

type RelationType string

const (
	TableRelation RelationType = "table"
	ViewRelation  RelationType = "view"
)

// RelationRef provides a reference to a database table
type RelationRef struct {
	Name    string       `json:"name"`              // the relation's name
	Schema  string       `json:"schema,omitempty"`  // the relation's schema
	Catalog string       `json:"catalog,omitempty"` // the relation's catalog
	Type    RelationType `json:"type,omitempty"`    // the relation's type
}

func (t RelationRef) String() string {
	if t.Catalog != "" && t.Schema != "" {
		return fmt.Sprintf("%s.%s.%s", t.Catalog, t.Schema, t.Name)
	}
	if t.Schema != "" {
		return fmt.Sprintf("%s.%s", t.Schema, t.Name)
	}

	return t.Name
}

func (r *RelationRef) UnmarshalJSON(data []byte) error {
	var rawRelationRef struct {
		Name    string       `json:"name"`
		Schema  string       `json:"schema,omitempty"`
		Catalog string       `json:"catalog,omitempty"`
		Type    RelationType `json:"type"`
	}
	err := json.Unmarshal(data, &rawRelationRef)
	if err != nil {
		return fmt.Errorf("failed to unmarshal RelationRef: %w", err)
	}
	if rawRelationRef.Type == "" {
		rawRelationRef.Type = TableRelation
	}

	*r = NewRelationRef(rawRelationRef.Name, WithSchema(rawRelationRef.Schema), WithCatalog(rawRelationRef.Catalog), WithRelationType(rawRelationRef.Type))
	return nil
}
