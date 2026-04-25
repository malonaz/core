package grpc

import (
	"context"

	"google.golang.org/grpc/metadata"
)

func GetHTTPMethod(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(HeaderXHTTPMethod)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
