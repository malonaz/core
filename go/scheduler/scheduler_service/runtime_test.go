package scheduler_service

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/pbutil"
)

func newOpts(fileDescriptorSets ...string) *Opts {
	return &Opts{
		FileDescriptorSets: fileDescriptorSets,
		MaxParallelJobs:    1,
		PollInterval:       time.Second,
		LeaseDuration:      time.Second,
		SweepInterval:      time.Second,
		WorkerID:           "test",
	}
}

// writeFileDescriptorSet writes the scheduler service's own descriptors,
// including their imports, as a descriptor set file.
func writeFileDescriptorSet(t *testing.T) string {
	t.Helper()
	fileDescriptorSet := &descriptorpb.FileDescriptorSet{}
	pathSet := map[string]struct{}{}
	var walk func(path string)
	walk = func(path string) {
		if _, ok := pathSet[path]; ok {
			return
		}
		pathSet[path] = struct{}{}
		file, err := protoregistry.GlobalFiles.FindFileByPath(path)
		require.NoError(t, err)
		imports := file.Imports()
		for i := 0; i < imports.Len(); i++ {
			walk(imports.Get(i).Path())
		}
		fileDescriptorSet.File = append(fileDescriptorSet.File, protodesc.ToFileDescriptorProto(file))
	}
	walk(pb.File_malonaz_scheduler_scheduler_service_v1_scheduler_service_proto.Path())
	bytes, err := pbutil.Marshal(fileDescriptorSet)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "scheduler.bin")
	require.NoError(t, os.WriteFile(path, bytes, 0o600))
	return path
}

func TestNewRuntime_RequiresFileDescriptorSet(t *testing.T) {
	_, err := newRuntime(newOpts())
	require.ErrorContains(t, err, "--file-descriptor-set")

	_, err = newRuntime(newOpts(filepath.Join(t.TempDir(), "missing.bin")))
	require.ErrorContains(t, err, "missing.bin")
}

func TestCreateQueue_RejectsMethodAbsentFromDescriptorSet(t *testing.T) {
	// The ':services' suffix ai-engine takes is accepted and ignored.
	runtime, err := newRuntime(newOpts(writeFileDescriptorSet(t) + ":malonaz.scheduler.scheduler_service.v1.SchedulerService"))
	require.NoError(t, err)
	service := &Service{runtime: runtime}

	newRequest := func(method string) *pb.CreateQueueRequest {
		return &pb.CreateQueueRequest{Queue: &schedulerpb.Queue{
			Policy:   &schedulerpb.QueuePolicy{AttemptTimeout: durationpb.New(time.Second), MaxAttempts: 1},
			Handlers: []*schedulerpb.Handler{{Method: method, Target: "targets/x"}},
		}}
	}
	_, err = service.CreateQueue(context.Background(), newRequest("/malonaz.scheduler.scheduler_service.v1.SchedulerService/NoSuchMethod"))
	grpcrequire.Error(t, codes.InvalidArgument, err)
	_, err = service.CreateQueue(context.Background(), newRequest("/malonaz.scheduler.scheduler_service.v1.Job/GetJob"))
	grpcrequire.Error(t, codes.InvalidArgument, err)

	// A method in the set resolves, with its request and response types.
	method, err := runtime.resolveMethod("/malonaz.scheduler.scheduler_service.v1.SchedulerService/GetJob")
	require.NoError(t, err)
	require.Equal(t, "malonaz.scheduler.scheduler_service.v1.GetJobRequest", string(method.Input().FullName()))
	require.Equal(t, "malonaz.scheduler.v1.Job", string(method.Output().FullName()))
}
