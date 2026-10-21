package postgres

import (
	"context"
)

type readReplicaKeyType string

const readReplicaKey = readReplicaKeyType("read-replica")

// WithReadReplica marks the context so reads outside a transaction are served from the read replica.
func WithReadReplica(ctx context.Context) context.Context {
	return context.WithValue(ctx, readReplicaKey, true)
}

// ReadReplicaFromContext returns true if the context was marked with WithReadReplica.
func ReadReplicaFromContext(ctx context.Context) bool {
	readReplica, _ := ctx.Value(readReplicaKey).(bool)
	return readReplica
}
