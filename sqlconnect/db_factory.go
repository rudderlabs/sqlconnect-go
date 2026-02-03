package sqlconnect

import (
	"encoding/json"
	"fmt"
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
