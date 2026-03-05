package sqlconnect

// SchemaRef provides a reference to a database schema
type SchemaRef struct {
	Name string `json:"name"` // the schema
}

func (s SchemaRef) String() string {
	return s.Name
}
