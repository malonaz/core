package scheduler

import (
	"time"

	codepb "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	policypb "github.com/malonaz/core/genproto/scheduler/policy/v1"
)

var defaultRetryBackoff = &policypb.RetryBackoff{
	Initial:    durationpb.New(10 * time.Second),
	Max:        durationpb.New(10 * time.Minute),
	Multiplier: 2,
}

// defaultRetryableCodes are the transient failures worth another attempt;
// anything else means the request itself is wrong and would fail again.
var defaultRetryableCodes = []codepb.Code{
	codepb.Code_UNAVAILABLE, codepb.Code_INTERNAL, codepb.Code_UNKNOWN,
	codepb.Code_DEADLINE_EXCEEDED, codepb.Code_RESOURCE_EXHAUSTED, codepb.Code_ABORTED,
}

// NormalizePolicy fills a queue policy's defaults in: what the scheduler
// stores and runs, so a declared policy compares equal to the stored one.
func NormalizePolicy(policy *policypb.QueuePolicy) *policypb.QueuePolicy {
	policy = proto.CloneOf(policy)
	if policy.RetryBackoff == nil {
		policy.RetryBackoff = defaultRetryBackoff
	}
	if len(policy.RetryableCodes) == 0 {
		policy.RetryableCodes = defaultRetryableCodes
	}
	return policy
}

// MethodPath returns the gRPC path of a method, "/{service}/{method}", as
// grpc.Invoke and job.method carry it.
func MethodPath(service, method string) string {
	return "/" + service + "/" + method
}
