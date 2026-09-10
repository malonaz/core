// Package transition holds what both writers of a job's state share: the API
// (cancel, retry, expire) and the dispatcher (claim, complete). A transition
// mutates the job's proto form, restamps it, and records the outcome.
package transition

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/scheduler/model"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/aip"
)

// maxRecordedAttempts bounds the attempt history kept in a job's metadata.
const maxRecordedAttempts = 20

var transitionCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "scheduler",
	Subsystem: "job",
	Name:      "transitions_total",
	Help:      "Outcomes recorded, by queue, method and resulting state.",
}, []string{"queue", "method", "state"})

// WorkerColumns are the columns a transition writes; leaving `labels` out
// keeps a client's concurrent UpdateJob from being overwritten.
var WorkerColumns = []string{
	"state", "schedule_time", "start_time", "complete_time", "lock_time",
	"attempt_count", "error", "response", "purge_time", "metadata", "update_time", "etag",
}

// Now returns the current time at Postgres' timestamp precision, so a
// transition's in-memory row matches what a later read returns.
func Now() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

// IsTerminal reports whether no further transition applies to the state.
func IsTerminal(state schedulerpb.JobState) bool {
	switch state {
	case schedulerpb.JobState_JOB_STATE_SUCCEEDED, schedulerpb.JobState_JOB_STATE_FAILED, schedulerpb.JobState_JOB_STATE_CANCELLED:
		return true
	}
	return false
}

// Mutate applies fn to the job's proto form, restamps update_time and etag,
// and writes the result back into the model.
func Mutate(job *model.Job, now time.Time, fn func(*schedulerpb.Job) error) error {
	jobPb, err := job.ToPb()
	if err != nil {
		return err
	}
	if err := fn(jobPb); err != nil {
		return err
	}
	jobPb.UpdateTime = timestamppb.New(now)
	if jobPb.Etag, err = aip.ComputeETag(jobPb); err != nil {
		return err
	}
	mutated, err := model.JobFromPb(jobPb)
	if err != nil {
		return err
	}
	*job = *mutated
	return nil
}

// RecordAttempt closes the job's current attempt in its history with the given
// outcome (nil on success) and releases the worker.
func RecordAttempt(job *schedulerpb.Job, now time.Time, err error, retryDelay *durationpb.Duration) {
	metadata := job.GetMetadata()
	if metadata == nil {
		metadata = &schedulerpb.JobMetadata{}
	}
	attempt := &schedulerpb.JobAttempt{
		Attempt:    job.GetAttemptCount(),
		StartTime:  job.GetStartTime(),
		EndTime:    timestamppb.New(now),
		Worker:     metadata.GetWorker(),
		RetryDelay: retryDelay,
	}
	if err != nil {
		// Details may carry types this binary cannot resolve, which the JSON column cannot hold.
		attemptStatus := grpcstatus.Convert(err)
		attempt.Error = &statuspb.Status{Code: int32(attemptStatus.Code()), Message: attemptStatus.Message()}
	}
	metadata.Attempts = append(metadata.Attempts, attempt)
	if len(metadata.Attempts) > maxRecordedAttempts {
		metadata.Attempts = metadata.Attempts[len(metadata.Attempts)-maxRecordedAttempts:]
	}
	metadata.Worker = ""
	job.Metadata = metadata
}

// Observe counts a job entering state.
func Observe(job *schedulerpb.Job, state schedulerpb.JobState) {
	transitionCounter.WithLabelValues(job.GetQueue(), job.GetMethod(), state.String()).Inc()
}

// PurgeTime returns when a job completing now is deleted under the retention;
// nil keeps it forever.
func PurgeTime(now time.Time, retention time.Duration) *timestamppb.Timestamp {
	if retention <= 0 {
		return nil
	}
	return timestamppb.New(now.Add(retention))
}

// JobName returns the job's resource name under whichever parent it has.
func JobName(job *model.Job) string {
	switch {
	case job.UserID != nil:
		return (&schedulerpb.OrganizationsUsersJobResourceName{Organization: *job.OrganizationID, User: *job.UserID, Job: job.JobID}).String()
	case job.OrganizationID != nil:
		return (&schedulerpb.OrganizationsJobResourceName{Organization: *job.OrganizationID, Job: job.JobID}).String()
	}
	return (&schedulerpb.JobResourceName{Job: job.JobID}).String()
}
