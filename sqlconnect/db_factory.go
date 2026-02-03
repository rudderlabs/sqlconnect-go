package sqlconnect

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// NewDB creates a new database client for the provided name.
func NewDB(name string, credentialsJSON json.RawMessage) (DB, error) {
	factory, ok := dbfactories[name]
	if !ok {
		return nil, fmt.Errorf("unknown client factory: %s", name)
	}
	return factory(credentialsJSON)
}

type DBFactory func(credentialsJSON json.RawMessage) (DB, error)

var dbfactories = map[string]DBFactory{}

func RegisterDBFactory(name string, factory DBFactory) {
	dbfactories[name] = factory
}

// NewDialect creates a new database dialect for the provided name.
func NewDialect(name string, optionsJSON json.RawMessage) (Dialect, error) {
	factory, ok := dialectFactories[name]
	if !ok {
		return nil, fmt.Errorf("unknown dialect %s, available dialects are: %s", name, strings.Join(slices.Collect(maps.Keys(dialectFactories)), ", "))
	}
	return factory(optionsJSON)
}

type DialectFactory func(optionsJSON json.RawMessage) (Dialect, error)

var dialectFactories = map[string]DialectFactory{}

func RegisterDialectFactory(name string, factory DialectFactory) {
	dialectFactories[name] = factory
}
