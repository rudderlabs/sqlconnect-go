// Package chctx marks a context that carries a caller statement settings map,
// so the driver's connection wrapper leaves that map in place.
package chctx

import "context"

type key struct{}

// Mark returns a child of ctx that Has reports as marked.
func Mark(ctx context.Context) context.Context { return context.WithValue(ctx, key{}, true) }

// Has reports whether ctx or a parent was marked by Mark.
func Has(ctx context.Context) bool {
	v, _ := ctx.Value(key{}).(bool)
	return v
}
