package longrunning

import (
	"context"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	aippb "github.com/malonaz/core/genproto/aip/v1"
	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// ImportProgress is the ImportMetadata of a running import (AIP-153), reported
// to the scheduler as it changes. Every method returns the error of the report
// only when the job is no longer running, which is how the import notices a
// cancellation; a report lost to anything else is not a lost import.
type ImportProgress struct {
	client   schedulerservicepb.SchedulerServiceClient
	metadata *aippb.ImportMetadata
}

// NewImportProgress starts the tally of an import at zero.
func NewImportProgress(client schedulerservicepb.SchedulerServiceClient) *ImportProgress {
	return &ImportProgress{client: client, metadata: &aippb.ImportMetadata{}}
}

// Metadata is the tally so far.
func (p *ImportProgress) Metadata() *aippb.ImportMetadata { return p.metadata }

// SetTotal records how many items the source holds.
func (p *ImportProgress) SetTotal(ctx context.Context, total int32) error {
	p.metadata.TotalCount = total
	return p.report(ctx)
}

// Succeeded records count more imported resources.
func (p *ImportProgress) Succeeded(ctx context.Context, count int32) error {
	p.metadata.SuccessCount += count
	return p.report(ctx)
}

// Failed records an item that could not be imported (AIP-193).
func (p *ImportProgress) Failed(ctx context.Context, err error) error {
	p.metadata.FailureCount++
	p.metadata.Errors = append(p.metadata.Errors, grpcstatus.Convert(err).Proto())
	return p.report(ctx)
}

func (p *ImportProgress) report(ctx context.Context) error {
	if err := ReportProgress(ctx, p.client, p.metadata); status.HasCode(err, codes.FailedPrecondition) {
		return err
	}
	return nil
}
