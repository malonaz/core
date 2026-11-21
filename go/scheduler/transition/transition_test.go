package transition

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	policypb "github.com/malonaz/core/genproto/scheduler/policy/v1"
)

func TestPurgeTime(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	threeDays := &policypb.QueuePolicy{Retention: &policypb.QueuePolicy_RetentionDuration{RetentionDuration: durationpb.New(72 * time.Hour)}}
	forever := &policypb.QueuePolicy{Retention: &policypb.QueuePolicy_RetainForever{RetainForever: true}}
	for name, test := range map[string]struct {
		policy           *policypb.QueuePolicy
		defaultRetention time.Duration
		want             time.Duration // 0 expects no purge time.
	}{
		"queue duration overrides the default":  {policy: threeDays, defaultRetention: 720 * time.Hour, want: 72 * time.Hour},
		"queue duration when the default keeps": {policy: threeDays, want: 72 * time.Hour},
		"queue forever overrides the default":   {policy: forever, defaultRetention: 720 * time.Hour},
		"no queue retention takes the default":  {policy: &policypb.QueuePolicy{}, defaultRetention: 720 * time.Hour, want: 720 * time.Hour},
		"no queue takes the default":            {defaultRetention: 720 * time.Hour, want: 720 * time.Hour},
		"no retention anywhere keeps the job":   {policy: &policypb.QueuePolicy{}},
	} {
		t.Run(name, func(t *testing.T) {
			purgeTime := PurgeTime(now, test.policy, test.defaultRetention)
			if test.want == 0 {
				require.Nil(t, purgeTime)
				return
			}
			require.Equal(t, now.Add(test.want), purgeTime.AsTime())
		})
	}
}
