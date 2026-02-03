package sqlconnect

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
)

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
