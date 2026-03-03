package sqlconnect

// SchemaRef provides a reference to a database schema
type SchemaRef struct {
	Name    string `json:"name"`              // the schema
	Catalog string `json:"catalog,omitempty"` // the catalog/database
}

func (s SchemaRef) String() string {
	if s.Catalog != "" {
		return s.Catalog + "." + s.Name
	}
	return s.Name
}
