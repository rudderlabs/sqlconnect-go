package sqlconnect

// ColumnRef provides a reference to a table column
type ColumnRef struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	RawType string `json:"rawType"`
}
