package node_service

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	nodeservicepb "github.com/malonaz/core/genproto/workspace/node_service/v1"
	pb "github.com/malonaz/core/genproto/workspace/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

// SnapshotFilesystem implements the interface: it uploads what changed in the node's copy since
// its last snapshot, or nothing when nothing did, and returns the snapshot with its paths.
func (s *Service) SnapshotFilesystem(ctx context.Context, request *nodeservicepb.SnapshotFilesystemRequest) (*pb.Snapshot, error) {
	snapshotRn, err := pb.ParseSnapshotRn(request.GetSnapshot().GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing snapshot name: %v", err).Err()
	}
	filesystem := snapshotRn.FilesystemRn().String()
	unlock := s.lockFilesystem(filesystem)
	defer unlock()

	state, err := s.readState(filesystem)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	if state == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no copy of %s on node %s", filesystem, s.opts.NodeName).Err()
	}
	for _, recorded := range state.GetRequests() {
		if recorded.GetRequestId() == request.GetRequestId() {
			return recorded.GetSnapshot(), nil
		}
	}

	root, err := os.OpenRoot(s.localPath(filesystem))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "opening copy: %v", err).Err()
	}
	defer root.Close()
	files, err := walkCopy(root)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	changes := diffCopy(state.GetFiles(), files)
	snapshot := proto.CloneOf(request.GetSnapshot())
	snapshot.Added, snapshot.Changed, snapshot.Deleted = filePaths(changes.added), filePaths(changes.changed), changes.deleted

	// A full snapshot keeps restores to about one: the first, and once increments pile up.
	full := state.Sequence == 0 ||
		int(state.Incrementals) >= s.opts.FullSnapshotTurns ||
		state.IncrementalBytes*int64(s.opts.FullSnapshotRatio) > state.FullBytes
	// An unchanged copy stores nothing: the turn still gets its empty snapshot.
	if !changes.empty() || state.Sequence == 0 {
		snapshotFiles := files
		if !full {
			snapshotFiles = slices.Concat(changes.added, changes.changed)
			sortFiles(snapshotFiles)
		}
		header := &snapshotHeader{Snapshot: snapshot.GetName(), Full: full, Deleted: changes.deleted}
		size, err := s.uploadSnapshot(ctx, root, objectKey(filesystem, state.Sequence+1, full), header, snapshotFiles)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "uploading snapshot: %v", err).Err()
		}
		snapshot.SizeBytes = size
		state.Sequence++
		if full {
			state.FullBytes, state.Incrementals, state.IncrementalBytes = size, 0, 0
		} else {
			state.Incrementals++
			state.IncrementalBytes += size
		}
	}
	state.Files = files
	copyRequest := &nodeservicepb.CopyRequest{RequestId: request.GetRequestId(), Snapshot: snapshot}
	state.Requests = append(state.Requests, copyRequest)
	if excess := len(state.Requests) - s.opts.MaxRecordedRequests; excess > 0 {
		state.Requests = state.Requests[excess:]
	}
	if err := s.writeState(filesystem, state); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	return snapshot, nil
}

// uploadSnapshot writes the snapshot to a temporary file, uploads it to key and returns its size.
func (s *Service) uploadSnapshot(ctx context.Context, root *os.Root, key string, header *snapshotHeader, files []*nodeservicepb.CopyFile) (int64, error) {
	file, err := os.CreateTemp(filepath.Join(s.directory, temporaryDirectory), "snapshot-*")
	if err != nil {
		return 0, fmt.Errorf("creating temporary file: %w", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := writeSnapshot(root, file, header, files); err != nil {
		return 0, err
	}
	size, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("sizing snapshot: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("rewinding snapshot: %w", err)
	}
	if err := s.objects.Put(ctx, key, file); err != nil {
		return 0, err
	}
	return size, nil
}

// PurgeFilesystem implements the interface: it deletes the filesystem's snapshots and this node's
// copy. Copies on other nodes are found stale and replaced when next mounted.
func (s *Service) PurgeFilesystem(ctx context.Context, request *nodeservicepb.PurgeFilesystemRequest) (*emptypb.Empty, error) {
	filesystemRn, err := pb.ParseFilesystemRn(request.GetFilesystem())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing filesystem: %v", err).Err()
	}
	filesystem := filesystemRn.String()
	unlock := s.lockFilesystem(filesystem)
	defer unlock()

	listed, err := s.objects.List(ctx, objectPrefix(filesystem))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "%v", err).Err()
	}
	keys := make([]string, 0, len(listed))
	for _, listedObject := range listed {
		keys = append(keys, listedObject.Key)
	}
	if err := s.objects.Delete(ctx, keys); err != nil {
		return nil, status.Errorf(codes.Unavailable, "%v", err).Err()
	}
	if err := s.removeCopy(filesystem); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	return &emptypb.Empty{}, nil
}

// prepareLocal brings the node's copy of a LOCAL filesystem to its latest snapshot and returns its
// path: a copy already there is used as is, any other is replaced by a restore.
func (s *Service) prepareLocal(ctx context.Context, filesystem string) (string, error) {
	unlock := s.lockFilesystem(filesystem)
	defer unlock()
	path := s.localPath(filesystem)

	snapshotObjects, err := s.listSnapshotObjects(ctx, filesystem)
	if err != nil {
		return "", err
	}
	var latest int64
	if len(snapshotObjects) > 0 {
		latest = snapshotObjects[len(snapshotObjects)-1].sequence
	}
	state, err := s.readState(filesystem)
	if err != nil {
		return "", err
	}
	if state != nil && state.Sequence == latest {
		return path, nil
	}

	// Restore from the last full snapshot and the increments after it.
	if err := s.removeCopy(filesystem); err != nil {
		return "", err
	}
	start := len(snapshotObjects)
	for i := len(snapshotObjects) - 1; i >= 0; i-- {
		if snapshotObjects[i].full {
			start = i
			break
		}
	}
	if start == len(snapshotObjects) && len(snapshotObjects) > 0 {
		return "", fmt.Errorf("no full snapshot of %s", filesystem)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("creating copy's parent: %w", err)
	}
	// Restored aside and moved in whole: a failed restore leaves no copy to be taken for the latest.
	temporaryPath := filepath.Join(s.directory, temporaryDirectory, uuid.MustNewV7().String())
	if err := os.Mkdir(temporaryPath, 0o755); err != nil {
		return "", fmt.Errorf("creating copy: %w", err)
	}
	defer os.RemoveAll(temporaryPath)
	root, err := os.OpenRoot(temporaryPath)
	if err != nil {
		return "", fmt.Errorf("opening copy: %w", err)
	}
	defer root.Close()
	state = &nodeservicepb.Copy{Sequence: latest}
	for _, snapshotObject := range snapshotObjects[start:] {
		if err := s.extractObject(ctx, root, snapshotObject.key); err != nil {
			return "", err
		}
		if snapshotObject.full {
			state.FullBytes = snapshotObject.size
			continue
		}
		state.Incrementals++
		state.IncrementalBytes += snapshotObject.size
	}
	if state.Files, err = walkCopy(root); err != nil {
		return "", err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return "", fmt.Errorf("moving copy in: %w", err)
	}
	if err := s.writeState(filesystem, state); err != nil {
		return "", err
	}
	return path, nil
}

func (s *Service) extractObject(ctx context.Context, root *os.Root, key string) error {
	reader, err := s.objects.Get(ctx, key)
	if err != nil {
		return err
	}
	defer reader.Close()
	if err := extractSnapshot(root, reader); err != nil {
		return fmt.Errorf("restoring %s: %w", key, err)
	}
	return nil
}
