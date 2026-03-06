package sqlconnect

import "fmt"

type Option func(options *Options)

type Options struct {
	Schema  string
	Catalog string
	Type    RelationType
	Prefix  string
}

func WithSchema(schema string) Option {
	return func(options *Options) {
		options.Schema = schema
	}
}

func WithCatalog(catalog string) Option {
	return func(options *Options) {
		options.Catalog = catalog
	}
}

func WithRelationType(relationType RelationType) Option {
	return func(options *Options) {
		options.Type = relationType
	}
}

func WithPrefix(prefix string) Option {
	return func(options *Options) {
		options.Prefix = prefix
	}
}

func NewOptions(opts ...Option) Options {
	var o Options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

type TableListOptions struct {
	Catalog string
	Prefix  string
}

func NewTableListOptions(opts ...Option) (TableListOptions, error) {
	o := NewOptions(opts...)
	if o.Schema != "" {
		return TableListOptions{}, fmt.Errorf("schema is not supported for table listing: %s", o.Schema)
	}

	return TableListOptions{
		Catalog: o.Catalog,
		Prefix:  o.Prefix,
	}, nil
}

type FilterOptions struct {
	Catalog string
}

func NewFilterOptions(opts ...Option) (FilterOptions, error) {
	o := NewOptions(opts...)
	if o.Schema != "" {
		return FilterOptions{}, fmt.Errorf("schema is not supported for filtering: %s", o.Schema)
	}
	if o.Prefix != "" {
		return FilterOptions{}, fmt.Errorf("prefix is not supported for filtering: %s", o.Prefix)
	}
	if o.Type != "" {
		return FilterOptions{}, fmt.Errorf("type is not supported for filtering: %s", o.Type)
	}

	return FilterOptions{
		Catalog: o.Catalog,
	}, nil
}
