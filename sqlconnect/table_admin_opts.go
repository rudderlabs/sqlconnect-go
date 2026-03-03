package sqlconnect

// ListTableOption configures ListTables method behavior.
type ListTableOption func(*ListTableConfig)

// ListTableConfig holds configuration for table listing operations.
type ListTableConfig struct {
	Prefix string
}

// WithPrefix specifies a prefix filter for table listing.
func WithPrefix(prefix string) ListTableOption {
	return func(c *ListTableConfig) {
		c.Prefix = prefix
	}
}

// ApplyListTableOptions applies options and returns the config.
func ApplyListTableOptions(opts ...ListTableOption) ListTableConfig {
	var c ListTableConfig
	for _, opt := range opts {
		opt(&c)
	}
	return c
}
