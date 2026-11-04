package agent_service

import (
	"context"
	"time"
)

type Opts struct {
	HeartbeatTimeout time.Duration `long:"heartbeat-timeout" env:"HEARTBEAT_TIMEOUT" default:"30s" description:"How long an executor may stay silent before it goes offline and its agents are released"`
	SweepInterval    time.Duration `long:"sweep-interval" env:"SWEEP_INTERVAL" default:"5s" description:"How often heartbeat deadlines are enforced and pending agents placed"`
	RefusalCooldown  time.Duration `long:"refusal-cooldown" env:"REFUSAL_COOLDOWN" default:"1m" description:"How long placement avoids an executor for an agent it released"`
}

type runtime struct {
	broker broker
}

func newRuntime(opts *Opts) (*runtime, error) {
	return &runtime{broker: newLocalBroker()}, nil
}

func (s *Service) start(ctx context.Context) (func(), error) {
	return s.startSweeper(ctx), nil
}
