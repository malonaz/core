package postgres

import (
	"context"
)

type eventualConsistencyKeyType string

const eventualConsistencyKey = eventualConsistencyKeyType("eventual-consistency")

// WithEventualConsistency marks the context so reads outside a transaction are served from the read replica.
func WithEventualConsistency(ctx context.Context) context.Context {
	return context.WithValue(ctx, eventualConsistencyKey, true)
}

// EventualConsistencyFromContext returns true if the context was marked with WithEventualConsistency.
func EventualConsistencyFromContext(ctx context.Context) bool {
	eventualConsistency, _ := ctx.Value(eventualConsistencyKey).(bool)
	return eventualConsistency
}
