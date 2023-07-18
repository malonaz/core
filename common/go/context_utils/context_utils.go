package contextutils

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
)

// Get extracts the value with the given key from the given context if it exists.
func Get(ctx context.Context, key string) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}

	values := md.Get(key)
	if len(values) != 1 {
		return "", false
	}
	return values[0], true
}

// Append appends the given (key, value) pairs to the given context.
func Append(ctx context.Context, keyValuePairs map[string]string) context.Context {
	kv := make([]string, 0, 2*len(keyValuePairs))
	for key, value := range keyValuePairs {
		kv = append(kv, key, value)
	}
	return metadata.AppendToOutgoingContext(ctx, kv...)
}
