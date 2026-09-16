package sqlconnect

import "context"

type querySettingsKey struct{}

// WithQuerySettings attaches warehouse-specific settings to a query. Drivers
// may enforce correctness settings. The map is copied so callers can reuse it.
func WithQuerySettings(ctx context.Context, settings map[string]any) context.Context {
	copied := make(map[string]any, len(settings))
	for key, value := range settings {
		copied[key] = value
	}
	return context.WithValue(ctx, querySettingsKey{}, copied)
}

// QuerySettings returns a copy of the settings attached by WithQuerySettings,
// or nil when no settings have been attached.
func QuerySettings(ctx context.Context) map[string]any {
	settings, ok := ctx.Value(querySettingsKey{}).(map[string]any)
	if !ok {
		return nil
	}
	copied := make(map[string]any, len(settings))
	for key, value := range settings {
		copied[key] = value
	}
	return copied
}
