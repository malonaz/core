package middleware

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var (
	AllButHealth = selector.MatchFunc(func(ctx context.Context, callMeta interceptors.CallMeta) bool {
		return grpc_health_v1.Health_ServiceDesc.ServiceName != callMeta.Service
	})
)
