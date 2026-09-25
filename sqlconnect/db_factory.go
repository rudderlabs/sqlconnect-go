package sqlconnect

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"golang.org/x/oauth2"
)

var (
	errNilBigQueryTokenSource = errors.New("bigquery token source is nil")
	errNilDBOption            = errors.New("database option is nil")
)

// NewDB creates a new database client for the provided name. It returns an error
// if options are supplied for a factory that does not support them.
func NewDB(name string, credentialsJSON json.RawMessage, opts ...DBOption) (DB, error) {
	factoryOptions := dbOptions{}
	for _, opt := range opts {
		if opt == nil {
			return nil, errNilDBOption
		}
		if err := opt.apply(name, &factoryOptions); err != nil {
			return nil, err
		}
	}

	if factory, ok := dbFactoriesWithOptions[name]; ok {
		return factory(credentialsJSON, DBFactoryOptions(factoryOptions))
	}

	factory, ok := dbfactories[name]
	if !ok {
		return nil, fmt.Errorf("unknown client factory: %s", name)
	}
	if len(opts) > 0 {
		return nil, fmt.Errorf("client factory %s does not support options", name)
	}
	return factory(credentialsJSON)
}

// DBOption configures database client construction.
type DBOption interface {
	apply(string, *dbOptions) error
}

type dbOptions struct {
	bigQueryTokenSource oauth2.TokenSource
}

type withBigQueryTokenSource struct {
	tokenSource oauth2.TokenSource
}

func (o withBigQueryTokenSource) apply(name string, opts *dbOptions) error {
	if isNil(o.tokenSource) {
		return errNilBigQueryTokenSource
	}
	if name != "bigquery" {
		return fmt.Errorf("bigquery token source is only supported for bigquery, not %s", name)
	}
	opts.bigQueryTokenSource = o.tokenSource
	return nil
}

// WithBigQueryTokenSource configures BigQuery clients to authenticate with the
// provided OAuth2 token source instead of service account JSON credentials. The
// database config must then carry no credentials: NewDB returns an error rather
// than silently discarding a configured key. The token source must be safe for
// concurrent use, cache tokens (for example with oauth2.ReuseTokenSource), and
// provide tokens with BigQuery scopes. Nil and typed-nil token sources are rejected.
func WithBigQueryTokenSource(tokenSource oauth2.TokenSource) DBOption {
	return withBigQueryTokenSource{tokenSource: tokenSource}
}

// DBFactory creates a database client from its JSON configuration.
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

// RegisterDBFactory registers a database factory. It replaces any option-aware
// factory previously registered under the same name.
func RegisterDBFactory(name string, factory DBFactory) {
	delete(dbFactoriesWithOptions, name)
	dbfactories[name] = factory
}

// RegisterDBFactoryWithOptions registers a database factory that accepts factory options.
func RegisterDBFactoryWithOptions(name string, factory DBFactoryWithOptions) {
	delete(dbfactories, name)
	dbFactoriesWithOptions[name] = factory
}

func isNil(value any) bool {
	if value == nil {
		return true
	}

	reflectedValue := reflect.ValueOf(value)
	switch reflectedValue.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflectedValue.IsNil()
	default:
		return false
	}
}
