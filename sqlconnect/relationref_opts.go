package sqlconnect

type Option func(options *RelationRefOption)

type RelationRefOption struct {
	Schema  string
	Catalog string
	Type    RelationType
}

func WithSchema(schema string) Option {
	return func(options *RelationRefOption) {
		options.Schema = schema
	}
}

func WithCatalog(catalog string) Option {
	return func(options *RelationRefOption) {
		options.Catalog = catalog
	}
}

func WithRelationType(relationType RelationType) Option {
	return func(options *RelationRefOption) {
		options.Type = relationType
	}
}
