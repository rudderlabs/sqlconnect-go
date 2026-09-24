package sqlconnect

import (
	"encoding/json"
	"fmt"

	"google.golang.org/api/option"
)

// NewDB creates a new database client for the provided name.
func NewDB(name string, credentialsJSON json.RawMessage, opts ...DBOption) (DB, error) {
	factory, ok := dbfactories[name]
	if !ok {
		return nil, fmt.Errorf("unknown client factory: %s", name)
	}

	if factoryWithGoogleClientOptions, ok := dbFactoriesWithGoogleClientOptions[name]; ok {
		config := newDBConfig{}
		for _, opt := range opts {
			opt(&config)
		}
		return factoryWithGoogleClientOptions(credentialsJSON, config.googleClientOptions...)
	}
	return factory(credentialsJSON)
}

// DBOption configures database client construction.
type DBOption func(*newDBConfig)

type newDBConfig struct {
	googleClientOptions []option.ClientOption
}

// WithGoogleClientOptions passes Google client options to database factories
// that support them. Other database factories ignore these options.
func WithGoogleClientOptions(opts ...option.ClientOption) DBOption {
	return func(config *newDBConfig) {
		config.googleClientOptions = append(config.googleClientOptions, opts...)
	}
}

type DBFactory func(credentialsJSON json.RawMessage) (DB, error)

// GoogleClientOptionsDBFactory creates a database client using optional Google client options.
type GoogleClientOptionsDBFactory func(credentialsJSON json.RawMessage, opts ...option.ClientOption) (DB, error)

var (
	dbfactories                        = map[string]DBFactory{}
	dbFactoriesWithGoogleClientOptions = map[string]GoogleClientOptionsDBFactory{}
)

func RegisterDBFactory(name string, factory DBFactory) {
	dbfactories[name] = factory
	delete(dbFactoriesWithGoogleClientOptions, name)
}

// RegisterDBFactoryWithGoogleClientOptions registers a database factory that
// accepts Google client options supplied to NewDB. It is intended for database
// implementations built into sqlconnect; callers should use RegisterDBFactory
// for custom database types.
func RegisterDBFactoryWithGoogleClientOptions(name string, factory GoogleClientOptionsDBFactory) {
	dbfactories[name] = func(credentialsJSON json.RawMessage) (DB, error) {
		return factory(credentialsJSON)
	}
	dbFactoriesWithGoogleClientOptions[name] = factory
}
