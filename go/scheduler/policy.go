package scheduler

import (
	"fmt"
	"time"

	"buf.build/go/protovalidate"
	codepb "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	policypb "github.com/malonaz/core/genproto/scheduler/policy/v1"
)

// RetryBackoffOpts is the retry backoff of a queue whose policy declares none.
type RetryBackoffOpts struct {
	Initial    time.Duration `long:"initial" env:"INITIAL" default:"10s" description:"Wait before a job's second attempt"`
	Max        time.Duration `long:"max" env:"MAX" default:"10m" description:"Longest wait between two attempts"`
	Multiplier float64       `long:"multiplier" env:"MULTIPLIER" default:"2" description:"Factor applied to the wait after each failed attempt"`
}

// Proto returns the backoff as a queue policy holds it, checked against the policy's constraints.
func (o *RetryBackoffOpts) Proto() (*policypb.RetryBackoff, error) {
	retryBackoff := &policypb.RetryBackoff{
		Initial:    durationpb.New(o.Initial),
		Max:        durationpb.New(o.Max),
		Multiplier: o.Multiplier,
	}
	if err := protovalidate.Validate(retryBackoff); err != nil {
		return nil, fmt.Errorf("invalid default retry backoff: %w", err)
	}
	return retryBackoff, nil
}

// defaultRetryableCodes are the transient failures worth another attempt;
// anything else means the request itself is wrong and would fail again.
var defaultRetryableCodes = []codepb.Code{
	codepb.Code_UNAVAILABLE, codepb.Code_INTERNAL, codepb.Code_UNKNOWN,
	codepb.Code_DEADLINE_EXCEEDED, codepb.Code_RESOURCE_EXHAUSTED, codepb.Code_ABORTED,
}

// NormalizePolicy fills a queue policy's defaults in: what the scheduler
// stores and runs, so a declared policy compares equal to the stored one.
func NormalizePolicy(policy *policypb.QueuePolicy, defaultRetryBackoff *policypb.RetryBackoff) *policypb.QueuePolicy {
	policy = proto.CloneOf(policy)
	if policy.RetryBackoff == nil {
		policy.RetryBackoff = proto.CloneOf(defaultRetryBackoff)
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
