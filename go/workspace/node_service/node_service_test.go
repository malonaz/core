package node_service

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	nodeservicepb "github.com/malonaz/core/genproto/workspace/node_service/v1"
	pb "github.com/malonaz/core/genproto/workspace/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/uuid"
)

const testFilesystem = "organizations/o/users/u/filesystems/repo"

// fakeObjects is an in-memory bucket, shared by the nodes of a test.
type fakeObjects struct {
	mutex      sync.Mutex
	keyToBytes map[string][]byte
}

func newFakeObjects() *fakeObjects {
	return &fakeObjects{keyToBytes: map[string][]byte{}}
}

func (f *fakeObjects) List(ctx context.Context, prefix string) ([]object, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	var listed []object
	for key, value := range f.keyToBytes {
		if strings.HasPrefix(key, prefix) {
			listed = append(listed, object{Key: key, Size: int64(len(value))})
		}
	}
	return listed, nil
}

func (f *fakeObjects) Put(ctx context.Context, key string, file *os.File) error {
	value, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.keyToBytes[key] = value
	return nil
}

func (f *fakeObjects) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	value, ok := f.keyToBytes[key]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(value)), nil
}

func (f *fakeObjects) Delete(ctx context.Context, keys []string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	for _, key := range keys {
		delete(f.keyToBytes, key)
	}
	return nil
}

func (f *fakeObjects) keys() []string {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return slices.Sorted(maps.Keys(f.keyToBytes))
}

// bind is a bind mount the fake mounter made.
type bind struct {
	Source   string
	ReadOnly bool
}

// fakeMounter records binds instead of mounting.
type fakeMounter struct {
	mutex        sync.Mutex
	targetToBind map[string]bind
}

func (f *fakeMounter) Mounted(target string) (bool, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	_, ok := f.targetToBind[target]
	return ok, nil
}

func (f *fakeMounter) Bind(source, target string, readOnly bool) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.targetToBind[target] = bind{Source: source, ReadOnly: readOnly}
	return nil
}

func (f *fakeMounter) Unmount(target string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	delete(f.targetToBind, target)
	return nil
}

// testNode is one node's agent over its own disk.
type testNode struct {
	service *Service
	mounter *fakeMounter
	csi     *nodeServer
}

func newTestNode(t *testing.T, objects *fakeObjects) *testNode {
	t.Helper()
	service, err := New(&Opts{Directory: t.TempDir(), NodeName: "node"})
	require.NoError(t, err)
	mounter := &fakeMounter{targetToBind: map[string]bind{}}
	service.objects, service.mounter = objects, mounter
	require.NoError(t, service.createLayout())
	return &testNode{service: service, mounter: mounter, csi: &nodeServer{service: service}}
}

// publish mounts the test filesystem as a workspace pod would, and returns its copy's path.
func (n *testNode) publish(t *testing.T, kind string, readOnly bool) string {
	t.Helper()
	target := filepath.Join(t.TempDir(), "mount")
	request := &csi.NodePublishVolumeRequest{
		TargetPath:    target,
		Readonly:      readOnly,
		VolumeContext: map[string]string{filesystemAttribute: testFilesystem, kindAttribute: kind, "readOnly": "false"},
	}
	_, err := n.csi.NodePublishVolume(context.Background(), request)
	require.NoError(t, err)
	return n.mounter.targetToBind[target].Source
}

func (n *testNode) snapshot(t *testing.T, requestID string) (*pb.Snapshot, error) {
	t.Helper()
	request := &nodeservicepb.SnapshotFilesystemRequest{
		Snapshot:  &pb.Snapshot{Name: testFilesystem + "/snapshots/" + requestID, Trigger: pb.Snapshot_TRIGGER_TURN, Turn: "turn"},
		RequestId: requestID,
	}
	return n.service.SnapshotFilesystem(context.Background(), request)
}

// testFile is what a test compares of a path.
type testFile struct {
	Mode    fs.FileMode
	Content string
}

// readTree returns every path of a directory with its mode and content, or link target.
func readTree(t *testing.T, directory string) map[string]testFile {
	t.Helper()
	pathToFile := map[string]testFile{}
	walk := func(path string, entry fs.DirEntry, err error) error {
		require.NoError(t, err)
		if path == directory {
			return nil
		}
		relativePath, err := filepath.Rel(directory, path)
		require.NoError(t, err)
		info, err := entry.Info()
		require.NoError(t, err)
		file := testFile{Mode: info.Mode()}
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			file.Content, err = os.Readlink(path)
			require.NoError(t, err)
		case info.Mode().IsRegular():
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			file.Content = string(content)
		}
		pathToFile[relativePath] = file
		return nil
	}
	require.NoError(t, filepath.WalkDir(directory, walk))
	return pathToFile
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

// snapshotPaths returns a snapshot's added, changed and deleted paths.
func snapshotPaths(snapshot *pb.Snapshot) [3][]string {
	return [3][]string{snapshot.GetAdded(), snapshot.GetChanged(), snapshot.GetDeleted()}
}

func TestSnapshotAndRestore(t *testing.T) {
	objects := newFakeObjects()
	nodeA := newTestNode(t, objects)

	// A new filesystem mounts as an empty copy.
	copyA := nodeA.publish(t, kindLocal, false)
	require.Equal(t, nodeA.service.localPath(testFilesystem), copyA)
	require.Empty(t, readTree(t, copyA))

	writeTestFile(t, filepath.Join(copyA, "a.txt"), "a")
	writeTestFile(t, filepath.Join(copyA, "directory", "b.txt"), "b")
	require.NoError(t, os.Symlink("a.txt", filepath.Join(copyA, "link")))
	firstRequestID := uuid.MustNewV7().String()
	first, err := nodeA.snapshot(t, firstRequestID)
	require.NoError(t, err)
	require.Equal(t, [3][]string{{"a.txt", "directory", "directory/b.txt", "link"}, nil, nil}, snapshotPaths(first))
	require.Positive(t, first.GetSizeBytes())

	// A retry answers the first attempt's snapshot, though the copy changed since.
	writeTestFile(t, filepath.Join(copyA, "a.txt"), "a, changed")
	retried, err := nodeA.snapshot(t, firstRequestID)
	require.NoError(t, err)
	grpcrequire.Equal(t, first, retried)

	require.NoError(t, os.Remove(filepath.Join(copyA, "directory", "b.txt")))
	writeTestFile(t, filepath.Join(copyA, "c.txt"), "c")
	second, err := nodeA.snapshot(t, uuid.MustNewV7().String())
	require.NoError(t, err)
	require.Equal(t, [3][]string{{"c.txt"}, {"a.txt"}, {"directory/b.txt"}}, snapshotPaths(second))

	// An unchanged copy stores nothing.
	third, err := nodeA.snapshot(t, uuid.MustNewV7().String())
	require.NoError(t, err)
	require.Equal(t, [3][]string{}, snapshotPaths(third))
	require.Zero(t, third.GetSizeBytes())
	expectedKeys := []string{
		testFilesystem + "/00000000000000000001.full.tar.zst",
		testFilesystem + "/00000000000000000002.incremental.tar.zst",
	}
	require.Equal(t, expectedKeys, objects.keys())

	// Another node restores the full snapshot and its increment.
	nodeB := newTestNode(t, objects)
	copyB := nodeB.publish(t, kindLocal, false)
	require.Equal(t, readTree(t, copyA), readTree(t, copyB))

	// Node A finds its copy stale once B snapshots, and restores B's state.
	require.NoError(t, os.RemoveAll(filepath.Join(copyB, "directory")))
	fourth, err := nodeB.snapshot(t, uuid.MustNewV7().String())
	require.NoError(t, err)
	require.Equal(t, [3][]string{nil, nil, {"directory"}}, snapshotPaths(fourth))
	writeTestFile(t, filepath.Join(copyA, "unsnapshotted.txt"), "lost")
	copyA = nodeA.publish(t, kindLocal, false)
	require.Equal(t, readTree(t, copyB), readTree(t, copyA))
}

func TestSnapshotWritesFullAfterIncrementalsPileUp(t *testing.T) {
	objects := newFakeObjects()
	node := newTestNode(t, objects)
	copyPath := node.publish(t, kindLocal, false)
	// A big, unchanging file keeps every increment under the ratio.
	big := make([]byte, 1<<16)
	_, err := rand.Read(big)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(copyPath, "big.bin"), big, 0o644))
	// A full, fullSnapshotTurns incrementals, then a full again.
	var expectedKeys []string
	for sequence := int64(1); sequence <= fullSnapshotTurns+2; sequence++ {
		writeTestFile(t, filepath.Join(copyPath, "file.txt"), strings.Repeat("x", int(sequence)))
		_, err := node.snapshot(t, uuid.MustNewV7().String())
		require.NoError(t, err)
		full := sequence == 1 || sequence == fullSnapshotTurns+2
		expectedKeys = append(expectedKeys, objectKey(testFilesystem, sequence, full))
	}
	require.Equal(t, expectedKeys, objects.keys())

	// A restore starts from the latest full.
	delete(objects.keyToBytes, expectedKeys[0])
	restored := newTestNode(t, objects).publish(t, kindLocal, false)
	require.Equal(t, readTree(t, copyPath), readTree(t, restored))
}

func TestSnapshotFindsEditKeepingSizeAndModTime(t *testing.T) {
	node := newTestNode(t, newFakeObjects())
	copyPath := node.publish(t, kindLocal, false)
	path := filepath.Join(copyPath, "a.txt")
	writeTestFile(t, path, "a")
	info, err := os.Stat(path)
	require.NoError(t, err)
	_, err = node.snapshot(t, uuid.MustNewV7().String())
	require.NoError(t, err)

	writeTestFile(t, path, "b")
	require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
	snapshot, err := node.snapshot(t, uuid.MustNewV7().String())
	require.NoError(t, err)
	require.Equal(t, [3][]string{nil, {"a.txt"}, nil}, snapshotPaths(snapshot))
}

func TestDiffCopy(t *testing.T) {
	// `a.txt` sorts before `a/b` though a walk visits it after.
	previous := []*nodeservicepb.CopyFile{
		{Path: "a", Mode: uint32(fs.ModeDir)},
		{Path: "a.txt", Size: 1},
		{Path: "a/b", Size: 1},
		{Path: "gone"},
	}
	current := []*nodeservicepb.CopyFile{
		{Path: "a", Mode: uint32(fs.ModeDir)},
		{Path: "a.txt", Size: 2},
		{Path: "a/b", Size: 1},
		{Path: "a/c", Size: 1},
		{Path: "new"},
	}
	changes := diffCopy(previous, current)
	require.Equal(t, [3][]string{{"a/c", "new"}, {"a.txt"}, {"gone"}}, [3][]string{filePaths(changes.added), filePaths(changes.changed), changes.deleted})
}

func TestSnapshotWithoutCopy(t *testing.T) {
	node := newTestNode(t, newFakeObjects())
	_, err := node.snapshot(t, uuid.MustNewV7().String())
	grpcrequire.Error(t, codes.FailedPrecondition, err)
}

func TestPurgeFilesystem(t *testing.T) {
	objects := newFakeObjects()
	node := newTestNode(t, objects)
	otherNode := newTestNode(t, objects)
	copyPath := node.publish(t, kindLocal, false)
	writeTestFile(t, filepath.Join(copyPath, "a.txt"), "a")
	_, err := node.snapshot(t, uuid.MustNewV7().String())
	require.NoError(t, err)
	otherNode.publish(t, kindLocal, false)

	request := &nodeservicepb.PurgeFilesystemRequest{Filesystem: testFilesystem}
	for range 2 {
		_, err := node.service.PurgeFilesystem(context.Background(), request)
		require.NoError(t, err)
	}
	require.Empty(t, objects.keys())
	_, err = os.Lstat(copyPath)
	require.ErrorIs(t, err, fs.ErrNotExist)

	// The other node's copy is stale: a filesystem made again under the name starts empty.
	require.Empty(t, readTree(t, otherNode.publish(t, kindLocal, false)))
}

func TestPublishAndUnpublish(t *testing.T) {
	node := newTestNode(t, newFakeObjects())
	target := filepath.Join(t.TempDir(), "mount")
	publishRequest := &csi.NodePublishVolumeRequest{
		TargetPath:    target,
		Readonly:      true,
		VolumeContext: map[string]string{filesystemAttribute: testFilesystem, kindAttribute: kindShared},
	}
	// Publishing twice binds once.
	for range 2 {
		_, err := node.csi.NodePublishVolume(context.Background(), publishRequest)
		require.NoError(t, err)
	}
	expected := map[string]bind{target: {Source: node.service.sharedPath(testFilesystem), ReadOnly: true}}
	require.Equal(t, expected, node.mounter.targetToBind)
	require.DirExists(t, node.service.sharedPath(testFilesystem))

	unpublishRequest := &csi.NodeUnpublishVolumeRequest{TargetPath: target}
	for range 2 {
		_, err := node.csi.NodeUnpublishVolume(context.Background(), unpublishRequest)
		require.NoError(t, err)
	}
	require.Empty(t, node.mounter.targetToBind)
	require.NoDirExists(t, target)

	for _, volumeContext := range []map[string]string{
		{filesystemAttribute: "../escape", kindAttribute: kindLocal},
		{filesystemAttribute: testFilesystem, kindAttribute: "other"},
	} {
		invalidRequest := &csi.NodePublishVolumeRequest{TargetPath: target, VolumeContext: volumeContext}
		_, err := node.csi.NodePublishVolume(context.Background(), invalidRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	}
}

// writeTestSnapshot writes a snapshot of raw tar entries, as an attacker might.
func writeTestSnapshot(t *testing.T, header *snapshotHeader, entries []*tar.Header) []byte {
	t.Helper()
	buffer := &bytes.Buffer{}
	encoder, err := zstd.NewWriter(buffer)
	require.NoError(t, err)
	tarWriter := tar.NewWriter(encoder)
	headerBytes := []byte(`{"snapshot":"s","full":true}`)
	if len(header.Deleted) > 0 {
		headerBytes = []byte(`{"snapshot":"s","full":true,"deleted":["` + strings.Join(header.Deleted, `","`) + `"]}`)
	}
	require.NoError(t, tarWriter.WriteHeader(&tar.Header{Name: "snapshot.json", Mode: 0o644, Size: int64(len(headerBytes))}))
	_, err = tarWriter.Write(headerBytes)
	require.NoError(t, err)
	for _, entry := range entries {
		// Owned by the test's user, so only the escape can fail it.
		entry.Uid, entry.Gid = os.Getuid(), os.Getgid()
		require.NoError(t, tarWriter.WriteHeader(entry))
	}
	require.NoError(t, tarWriter.Close())
	require.NoError(t, encoder.Close())
	return buffer.Bytes()
}

func TestExtractStaysInsideCopy(t *testing.T) {
	outside := t.TempDir()
	expectedError := map[string]string{
		"parent path":         "path is not local",
		"through symlink":     "path escapes from parent",
		"deleted parent path": "is not local",
	}
	for name, snapshot := range map[string][]byte{
		"parent path": writeTestSnapshot(t, &snapshotHeader{}, []*tar.Header{{Name: "../escaped", Typeflag: tar.TypeReg, Mode: 0o644}}),
		"through symlink": writeTestSnapshot(t, &snapshotHeader{}, []*tar.Header{
			{Name: "link", Typeflag: tar.TypeSymlink, Linkname: outside},
			{Name: "link/escaped", Typeflag: tar.TypeReg, Mode: 0o644},
		}),
		"deleted parent path": writeTestSnapshot(t, &snapshotHeader{Deleted: []string{"../escaped"}}, nil),
	} {
		root, err := os.OpenRoot(t.TempDir())
		require.NoError(t, err)
		err = extractSnapshot(root, bytes.NewReader(snapshot))
		require.ErrorContains(t, err, expectedError[name], name)
		require.NoError(t, root.Close())
	}
	require.Empty(t, readTree(t, outside))
}

func TestUnescapeMountPoint(t *testing.T) {
	require.Equal(t, "/var/lib/a b", unescapeMountPoint(`/var/lib/a\040b`))
	require.Equal(t, "/plain", unescapeMountPoint("/plain"))
}
