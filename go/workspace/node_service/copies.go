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

	"github.com/malonaz/core/go/uuid"
)

const (
	// fullSuffix and incrementalSuffix end a snapshot object's key after its zero-padded sequence.
	fullSuffix        = ".full.tar.zst"
	incrementalSuffix = ".incremental.tar.zst"
	// sequenceDigits pads sequences so keys sort in order.
	sequenceDigits = 20
)

// copyState is what the node knows of its copy of a LOCAL filesystem, kept beside it.
type copyState struct {
	// Sequence is the snapshot object the copy was last at; 0 before the first.
	Sequence int64 `json:"sequence"`
	// FullBytes is the size of the last full snapshot.
	FullBytes int64 `json:"full_bytes"`
	// Incrementals and IncrementalBytes count the snapshots written since the last full.
	Incrementals     int   `json:"incrementals"`
	IncrementalBytes int64 `json:"incremental_bytes"`
	// PathToFile is every path in the copy at Sequence, relative to its root.
	PathToFile map[string]fileState `json:"path_to_file"`
	// Requests are the latest snapshot requests answered, oldest first.
	Requests []recordedRequest `json:"requests"`
}

// fileState is what tells a path's change apart, as git's index does. ChangeTime and Inode catch
// an edit that keeps size and mtime: workspace code can set mtime, never ctime.
type fileState struct {
	Mode       fs.FileMode `json:"mode"`
	Size       int64       `json:"size,omitempty"`
	ModTime    int64       `json:"mod_time,omitempty"`
	ChangeTime int64       `json:"change_time,omitempty"`
	Inode      uint64      `json:"inode,omitempty"`
	Link       string      `json:"link,omitempty"`
}

// recordedRequest is a snapshot request answered, to answer its retries alike.
type recordedRequest struct {
	RequestID string `json:"request_id"`
	Snapshot  []byte `json:"snapshot"`
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

// changes is how a copy differs from its last snapshot.
type changes struct {
	added, changed, deleted []string
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
	return filepath.Join(r.directory, stateDirectory, filesystem+".json")
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
func (r *runtime) readState(filesystem string) (*copyState, error) {
	bytes, err := os.ReadFile(r.statePath(filesystem))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading state: %w", err)
	}
	state := &copyState{}
	if err := json.Unmarshal(bytes, state); err != nil {
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
func (r *runtime) writeState(filesystem string, state *copyState) error {
	bytes, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encoding state: %w", err)
	}
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

// walkCopy returns every regular file, directory and symlink in a copy. Symlinks are not followed.
func walkCopy(root *os.Root) (map[string]fileState, error) {
	pathToFile := map[string]fileState{}
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
		file := fileState{Mode: info.Mode()}
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
			pathToFile[path] = file
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
		pathToFile[path] = file
		return nil
	}
	if err := fs.WalkDir(root.FS(), ".", walk); err != nil {
		return nil, fmt.Errorf("walking copy: %w", err)
	}
	return pathToFile, nil
}

// diffCopy returns what changed from previous to current, each list sorted.
func diffCopy(previous, current map[string]fileState) *changes {
	changes := &changes{}
	for path, file := range current {
		previousFile, ok := previous[path]
		switch {
		case !ok:
			changes.added = append(changes.added, path)
		case previousFile != file:
			changes.changed = append(changes.changed, path)
		}
	}
	for path := range previous {
		if _, ok := current[path]; !ok {
			changes.deleted = append(changes.deleted, path)
		}
	}
	slices.Sort(changes.added)
	slices.Sort(changes.changed)
	slices.Sort(changes.deleted)
	return changes
}

// writeSnapshot writes the paths of a copy, parents before children, as a zstd tar to file.
func writeSnapshot(root *os.Root, file *os.File, header *snapshotHeader, paths []string, pathToFile map[string]fileState) error {
	encoder, err := zstd.NewWriter(file, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return fmt.Errorf("creating zstd writer: %w", err)
	}
	tarWriter := tar.NewWriter(encoder)
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return fmt.Errorf("encoding snapshot header: %w", err)
	}
	snapshotTarHeader := &tar.Header{Name: "snapshot.json", Mode: 0o644, Size: int64(len(headerBytes)), Typeflag: tar.TypeReg, Format: tar.FormatPAX}
	if err := tarWriter.WriteHeader(snapshotTarHeader); err != nil {
		return fmt.Errorf("writing snapshot header: %w", err)
	}
	if _, err := tarWriter.Write(headerBytes); err != nil {
		return fmt.Errorf("writing snapshot header: %w", err)
	}
	for _, path := range paths {
		if err := writeSnapshotEntry(root, tarWriter, path, pathToFile[path]); err != nil {
			return fmt.Errorf("writing %s: %w", path, err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		return fmt.Errorf("closing tar: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return fmt.Errorf("closing zstd writer: %w", err)
	}
	return nil
}

func writeSnapshotEntry(root *os.Root, tarWriter *tar.Writer, path string, file fileState) error {
	if !file.Mode.IsRegular() {
		info, err := root.Lstat(path)
		if err != nil {
			return err
		}
		tarHeader, err := tar.FileInfoHeader(info, file.Link)
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
	if !info.Mode().IsRegular() || info.Size() != file.Size {
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
