package node_service

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/klauspost/compress/zstd"

	nodeservicepb "github.com/malonaz/core/genproto/workspace/node_service/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/uuid"
)

const (
	// fullSuffix and incrementalSuffix end a snapshot object's key after its zero-padded sequence.
	fullSuffix        = ".full.tar.zst"
	incrementalSuffix = ".incremental.tar.zst"
	// sequenceDigits pads sequences so keys sort in order.
	sequenceDigits = 20
)

// stateEncoder and stateDecoder compress copies' state; both are safe for concurrent use.
var (
	stateEncoder = must(zstd.NewWriter(nil))
	stateDecoder = must(zstd.NewReader(nil))
)

func must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

// snapshotHeader is the first entry of every snapshot tar.
type snapshotHeader struct {
	Snapshot string   `json:"snapshot"`
	Full     bool     `json:"full"`
	Deleted  []string `json:"deleted,omitempty"`
}

// snapshotObject is one stored snapshot of a filesystem.
type snapshotObject struct {
	key      string
	sequence int64
	full     bool
	size     int64
}

// changes is how a copy differs from its last snapshot, each list sorted by path.
type changes struct {
	added, changed []*nodeservicepb.CopyFile
	deleted        []string
}

func (c *changes) empty() bool {
	return len(c.added) == 0 && len(c.changed) == 0 && len(c.deleted) == 0
}

func (r *runtime) localPath(filesystem string) string {
	return filepath.Join(r.directory, localDirectory, filesystem)
}

func (r *runtime) sharedPath(filesystem string) string {
	return filepath.Join(r.directory, sharedDirectory, filesystem)
}

func (r *runtime) statePath(filesystem string) string {
	return filepath.Join(r.directory, stateDirectory, filesystem+".pb.zst")
}

// objectPrefix holds every snapshot object of a filesystem.
func objectPrefix(filesystem string) string {
	return filesystem + "/"
}

func objectKey(filesystem string, sequence int64, full bool) string {
	suffix := incrementalSuffix
	if full {
		suffix = fullSuffix
	}
	return fmt.Sprintf("%s%0*d%s", objectPrefix(filesystem), sequenceDigits, sequence, suffix)
}

// listSnapshotObjects returns a filesystem's snapshot objects in sequence order.
func (r *runtime) listSnapshotObjects(ctx context.Context, filesystem string) ([]snapshotObject, error) {
	prefix := objectPrefix(filesystem)
	listed, err := r.objects.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	snapshotObjects := make([]snapshotObject, 0, len(listed))
	for _, listedObject := range listed {
		name := strings.TrimPrefix(listedObject.Key, prefix)
		snapshotObject := snapshotObject{key: listedObject.Key, size: listedObject.Size}
		switch {
		case strings.HasSuffix(name, fullSuffix):
			snapshotObject.full = true
			name = strings.TrimSuffix(name, fullSuffix)
		case strings.HasSuffix(name, incrementalSuffix):
			name = strings.TrimSuffix(name, incrementalSuffix)
		default:
			return nil, fmt.Errorf("unknown snapshot object %s", listedObject.Key)
		}
		if snapshotObject.sequence, err = strconv.ParseInt(name, 10, 64); err != nil {
			return nil, fmt.Errorf("parsing sequence of %s: %w", listedObject.Key, err)
		}
		snapshotObjects = append(snapshotObjects, snapshotObject)
	}
	sort.Slice(snapshotObjects, func(i, j int) bool { return snapshotObjects[i].sequence < snapshotObjects[j].sequence })
	return snapshotObjects, nil
}

// readState returns the node's state of a filesystem's copy, or nil with no copy.
func (r *runtime) readState(filesystem string) (*nodeservicepb.Copy, error) {
	compressed, err := os.ReadFile(r.statePath(filesystem))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading state: %w", err)
	}
	bytes, err := stateDecoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompressing state: %w", err)
	}
	state := &nodeservicepb.Copy{}
	if err := pbutil.Unmarshal(bytes, state); err != nil {
		return nil, fmt.Errorf("decoding state: %w", err)
	}
	if _, err := os.Lstat(r.localPath(filesystem)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading copy: %w", err)
	}
	return state, nil
}

// writeState replaces the state of a filesystem's copy atomically.
func (r *runtime) writeState(filesystem string, state *nodeservicepb.Copy) error {
	bytes, err := pbutil.Marshal(state)
	if err != nil {
		return fmt.Errorf("encoding state: %w", err)
	}
	bytes = stateEncoder.EncodeAll(bytes, nil)
	path := r.statePath(filesystem)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating state directory: %w", err)
	}
	temporaryPath := filepath.Join(r.directory, temporaryDirectory, uuid.MustNewV7().String())
	if err := os.WriteFile(temporaryPath, bytes, 0o644); err != nil {
		return fmt.Errorf("writing state: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replacing state: %w", err)
	}
	return nil
}

// removeCopy removes a filesystem's copy and its state from the node. Missing ones are not an error.
func (r *runtime) removeCopy(filesystem string) error {
	if err := os.Remove(r.statePath(filesystem)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("removing state: %w", err)
	}
	// Moved aside first: a copy half removed is never taken for a whole one.
	temporaryPath := filepath.Join(r.directory, temporaryDirectory, uuid.MustNewV7().String())
	if err := os.Rename(r.localPath(filesystem), temporaryPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("moving copy aside: %w", err)
	}
	if err := os.RemoveAll(temporaryPath); err != nil {
		return fmt.Errorf("removing copy: %w", err)
	}
	return nil
}

// walkCopy returns every regular file, directory and symlink in a copy, sorted by path. Symlinks
// are not followed.
func walkCopy(root *os.Root) ([]*nodeservicepb.CopyFile, error) {
	var files []*nodeservicepb.CopyFile
	walk := func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == "." {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		file := &nodeservicepb.CopyFile{Path: path, Mode: uint32(info.Mode())}
		switch {
		case info.Mode().IsRegular():
			file.Size = info.Size()
			file.ModTime = info.ModTime().UnixNano()
		case info.Mode()&fs.ModeSymlink != 0:
			if file.Link, err = root.Readlink(path); err != nil {
				return err
			}
		case info.IsDir():
			// A directory's times move with its entries, which are diffed on their own.
			files = append(files, file)
			return nil
		default:
			// Sockets, pipes and devices are not kept.
			return nil
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("no stat for %s", path)
		}
		file.ChangeTime = stat.Ctim.Nano()
		file.Inode = stat.Ino
		files = append(files, file)
		return nil
	}
	if err := fs.WalkDir(root.FS(), ".", walk); err != nil {
		return nil, fmt.Errorf("walking copy: %w", err)
	}
	// WalkDir visits `a/b` before `a.txt`: sorted by path, two copies diff by merging.
	sortFiles(files)
	return files, nil
}

func sortFiles(files []*nodeservicepb.CopyFile) {
	slices.SortFunc(files, func(a, b *nodeservicepb.CopyFile) int { return strings.Compare(a.GetPath(), b.GetPath()) })
}

// diffCopy returns what changed from previous to current, merging the two sorted lists.
func diffCopy(previous, current []*nodeservicepb.CopyFile) *changes {
	changes := &changes{}
	i, j := 0, 0
	for i < len(previous) || j < len(current) {
		switch {
		case j == len(current) || (i < len(previous) && previous[i].GetPath() < current[j].GetPath()):
			changes.deleted = append(changes.deleted, previous[i].GetPath())
			i++
		case i == len(previous) || current[j].GetPath() < previous[i].GetPath():
			changes.added = append(changes.added, current[j])
			j++
		default:
			if !sameFile(previous[i], current[j]) {
				changes.changed = append(changes.changed, current[j])
			}
			i++
			j++
		}
	}
	return changes
}

// sameFile reports whether two states of one path are unchanged.
func sameFile(a, b *nodeservicepb.CopyFile) bool {
	return a.GetMode() == b.GetMode() && a.GetSize() == b.GetSize() && a.GetModTime() == b.GetModTime() &&
		a.GetChangeTime() == b.GetChangeTime() && a.GetInode() == b.GetInode() && a.GetLink() == b.GetLink()
}

// filePaths returns the paths of files, nil for none.
func filePaths(files []*nodeservicepb.CopyFile) []string {
	var paths []string
	for _, file := range files {
		paths = append(paths, file.GetPath())
	}
	return paths
}

// writeSnapshot writes files of a copy, sorted so parents come before children, as a zstd tar,
// and returns the tar's size before compression.
func writeSnapshot(root *os.Root, file *os.File, header *snapshotHeader, files []*nodeservicepb.CopyFile) (int64, error) {
	encoder, err := zstd.NewWriter(file, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return 0, fmt.Errorf("creating zstd writer: %w", err)
	}
	rawWriter := &countingWriter{writer: encoder}
	tarWriter := tar.NewWriter(rawWriter)
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return 0, fmt.Errorf("encoding snapshot header: %w", err)
	}
	snapshotTarHeader := &tar.Header{Name: "snapshot.json", Mode: 0o644, Size: int64(len(headerBytes)), Typeflag: tar.TypeReg, Format: tar.FormatPAX}
	if err := tarWriter.WriteHeader(snapshotTarHeader); err != nil {
		return 0, fmt.Errorf("writing snapshot header: %w", err)
	}
	if _, err := tarWriter.Write(headerBytes); err != nil {
		return 0, fmt.Errorf("writing snapshot header: %w", err)
	}
	for _, copyFile := range files {
		if err := writeSnapshotEntry(root, tarWriter, copyFile); err != nil {
			return 0, fmt.Errorf("writing %s: %w", copyFile.GetPath(), err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		return 0, fmt.Errorf("closing tar: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return 0, fmt.Errorf("closing zstd writer: %w", err)
	}
	return rawWriter.count, nil
}

func writeSnapshotEntry(root *os.Root, tarWriter *tar.Writer, file *nodeservicepb.CopyFile) error {
	path := file.GetPath()
	if !fs.FileMode(file.GetMode()).IsRegular() {
		info, err := root.Lstat(path)
		if err != nil {
			return err
		}
		tarHeader, err := tar.FileInfoHeader(info, file.GetLink())
		if err != nil {
			return err
		}
		tarHeader.Name = path
		tarHeader.Format = tar.FormatPAX
		return tarWriter.WriteHeader(tarHeader)
	}

	// O_NOFOLLOW: a file swapped for a symlink since the walk is refused, not read through.
	openedFile, err := root.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer openedFile.Close()
	info, err := openedFile.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != file.GetSize() {
		return fmt.Errorf("changed while snapshotting")
	}
	tarHeader, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return err
	}
	tarHeader.Name = path
	tarHeader.Format = tar.FormatPAX
	if err := tarWriter.WriteHeader(tarHeader); err != nil {
		return err
	}
	if _, err := io.CopyN(tarWriter, openedFile, info.Size()); err != nil {
		return err
	}
	return nil
}

// extractSnapshot applies one snapshot to a copy: its deletes, then its files.
func extractSnapshot(root *os.Root, reader io.Reader) error {
	decoder, err := zstd.NewReader(reader)
	if err != nil {
		return fmt.Errorf("creating zstd reader: %w", err)
	}
	defer decoder.Close()
	tarReader := tar.NewReader(decoder)
	if _, err := tarReader.Next(); err != nil {
		return fmt.Errorf("reading snapshot header: %w", err)
	}
	header := &snapshotHeader{}
	if err := json.NewDecoder(tarReader).Decode(header); err != nil {
		return fmt.Errorf("decoding snapshot header: %w", err)
	}
	for _, path := range header.Deleted {
		if !filepath.IsLocal(path) {
			return fmt.Errorf("deleted path %q is not local", path)
		}
		if err := root.RemoveAll(path); err != nil {
			return fmt.Errorf("deleting %s: %w", path, err)
		}
	}
	for {
		tarHeader, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading tar: %w", err)
		}
		if err := extractEntry(root, tarReader, tarHeader); err != nil {
			return fmt.Errorf("extracting %s: %w", tarHeader.Name, err)
		}
	}
}

// extractEntry writes one tar entry into a copy. The root keeps every write inside the copy,
// whatever symlinks its earlier entries made.
func extractEntry(root *os.Root, tarReader *tar.Reader, tarHeader *tar.Header) error {
	path := filepath.Clean(tarHeader.Name)
	if !filepath.IsLocal(path) {
		return fmt.Errorf("path is not local")
	}
	mode := fs.FileMode(tarHeader.Mode).Perm()
	if parent := filepath.Dir(path); parent != "." {
		if err := root.MkdirAll(parent, 0o755); err != nil {
			return err
		}
	}
	info, err := root.Lstat(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	exists := err == nil
	// A path changing type is replaced; a directory staying one keeps its entries.
	if exists && !(tarHeader.Typeflag == tar.TypeDir && info.IsDir()) {
		if err := root.RemoveAll(path); err != nil {
			return err
		}
		exists = false
	}

	switch tarHeader.Typeflag {
	case tar.TypeDir:
		if !exists {
			if err := root.Mkdir(path, mode); err != nil {
				return err
			}
		}
		if err := root.Chmod(path, mode); err != nil {
			return err
		}
	case tar.TypeSymlink:
		if err := root.Symlink(tarHeader.Linkname, path); err != nil {
			return err
		}
	case tar.TypeReg:
		file, err := root.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, mode)
		if err != nil {
			return err
		}
		if _, err := io.CopyN(file, tarReader, tarHeader.Size); err != nil {
			file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
		if err := root.Chmod(path, mode); err != nil {
			return err
		}
		if err := root.Chtimes(path, tarHeader.AccessTime, tarHeader.ModTime); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported entry type %q", tarHeader.Typeflag)
	}
	return root.Lchown(path, tarHeader.Uid, tarHeader.Gid)
}
