package longrunning

import (
	"context"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

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
	return reportProgress(ctx, p.client, p.metadata)
}

// Succeeded records count more imported resources.
func (p *ImportProgress) Succeeded(ctx context.Context, count int32) error {
	p.metadata.SuccessCount += count
	return reportProgress(ctx, p.client, p.metadata)
}

// Failed records an item that could not be imported (AIP-193).
func (p *ImportProgress) Failed(ctx context.Context, err error) error {
	p.metadata.FailureCount++
	p.metadata.Errors = append(p.metadata.Errors, grpcstatus.Convert(err).Proto())
	return reportProgress(ctx, p.client, p.metadata)
}

// ExportProgress is the ExportMetadata of a running export (AIP-153), reported
// like ImportProgress.
type ExportProgress struct {
	client   schedulerservicepb.SchedulerServiceClient
	metadata *aippb.ExportMetadata
}

// NewExportProgress starts the tally of an export at zero.
func NewExportProgress(client schedulerservicepb.SchedulerServiceClient) *ExportProgress {
	return &ExportProgress{client: client, metadata: &aippb.ExportMetadata{}}
}

// Metadata is the tally so far.
func (p *ExportProgress) Metadata() *aippb.ExportMetadata { return p.metadata }

// SetTotal records how many items the export holds.
func (p *ExportProgress) SetTotal(ctx context.Context, total int32) error {
	p.metadata.TotalCount = total
	return reportProgress(ctx, p.client, p.metadata)
}

// Succeeded records count more exported items.
func (p *ExportProgress) Succeeded(ctx context.Context, count int32) error {
	p.metadata.SuccessCount += count
	return reportProgress(ctx, p.client, p.metadata)
}

// Failed records an item counted as exported that the destination could not
// write after all (AIP-193): it moves from the successes to the failures.
func (p *ExportProgress) Failed(ctx context.Context, err error) error {
	if p.metadata.SuccessCount > 0 {
		p.metadata.SuccessCount--
	}
	p.metadata.FailureCount++
	p.metadata.Errors = append(p.metadata.Errors, grpcstatus.Convert(err).Proto())
	return reportProgress(ctx, p.client, p.metadata)
}

// reportProgress surfaces only a FailedPrecondition: the job is no longer running.
func reportProgress(ctx context.Context, client schedulerservicepb.SchedulerServiceClient, metadata proto.Message) error {
	if err := ReportProgress(ctx, client, metadata); status.HasCode(err, codes.FailedPrecondition) {
		return err
	}
	return nil
}
