package sqlconnect

// CatalogRef provides a reference to a database catalog
type CatalogRef struct {
	Name string `json:"name"` // the catalog/database name
}

func (c CatalogRef) String() string {
	return c.Name
}
