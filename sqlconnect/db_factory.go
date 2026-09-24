package sqlconnect

import (
	"encoding/json"
	"fmt"

	"golang.org/x/oauth2"
)

// NewDB creates a new database client for the provided name.
func NewDB(name string, credentialsJSON json.RawMessage, opts ...DBOption) (DB, error) {
	if factory, ok := dbFactoriesWithOptions[name]; ok {
		factoryOptions := dbOptions{}
		for _, opt := range opts {
			opt.apply(&factoryOptions)
		}
		return factory(credentialsJSON, DBFactoryOptions(factoryOptions))
	}

	factory, ok := dbfactories[name]
	if !ok {
		return nil, fmt.Errorf("unknown client factory: %s", name)
	}
	return factory(credentialsJSON)
}

// DBOption configures database client construction.
type DBOption interface {
	apply(*dbOptions)
}

type dbOptions struct {
	bigQueryTokenSource oauth2.TokenSource
}

type withBigQueryTokenSource struct {
	tokenSource oauth2.TokenSource
}

func (o withBigQueryTokenSource) apply(opts *dbOptions) {
	opts.bigQueryTokenSource = o.tokenSource
}

// WithBigQueryTokenSource configures BigQuery clients to authenticate with the
// provided OAuth2 token source. When supplied, it replaces the default service
// account JSON credentials option. Credentials JSON in the database config is
// still validated as service-account-only before this token source is used.
func WithBigQueryTokenSource(tokenSource oauth2.TokenSource) DBOption {
	return withBigQueryTokenSource{tokenSource: tokenSource}
}

type DBFactory func(credentialsJSON json.RawMessage) (DB, error)

// DBFactoryOptions contains options for database factories that support them.
type DBFactoryOptions struct {
	bigQueryTokenSource oauth2.TokenSource
}

// BigQueryTokenSource returns the caller-supplied token source for BigQuery, if any.
func (o DBFactoryOptions) BigQueryTokenSource() oauth2.TokenSource {
	return o.bigQueryTokenSource
}

// DBFactoryWithOptions creates a database client with factory options.
type DBFactoryWithOptions func(credentialsJSON json.RawMessage, opts DBFactoryOptions) (DB, error)

var (
	dbfactories            = map[string]DBFactory{}
	dbFactoriesWithOptions = map[string]DBFactoryWithOptions{}
)

func RegisterDBFactory(name string, factory DBFactory) {
	delete(dbFactoriesWithOptions, name)
	dbfactories[name] = factory
}

// RegisterDBFactoryWithOptions registers a database factory that accepts factory options.
func RegisterDBFactoryWithOptions(name string, factory DBFactoryWithOptions) {
	delete(dbfactories, name)
	dbFactoriesWithOptions[name] = factory
}
