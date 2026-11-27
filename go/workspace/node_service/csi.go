package node_service

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/wrapperspb"

	pb "github.com/malonaz/core/genproto/workspace/v1"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	// driverName is the CSI driver workspace pods' volumes name.
	driverName = "workspaces.onikisu"
	// The volume attributes a workspace pod's CSI volume carries.
	filesystemAttribute = "filesystem"
	kindAttribute       = "kind"
	kindLocal           = "local"
	kindShared          = "shared"
)

// identityServer tells kubelet the plugin's name and that it is ready.
type identityServer struct {
	csi.UnimplementedIdentityServer
}

// GetPluginInfo implements csi.IdentityServer.
func (identityServer) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: driverName, VendorVersion: "v1"}, nil
}

// GetPluginCapabilities implements csi.IdentityServer: a node plugin only, with no controller.
func (identityServer) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

// Probe implements csi.IdentityServer.
func (identityServer) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}

// nodeServer mounts filesystems into workspace pods: each pod has an inline volume per mount.
type nodeServer struct {
	csi.UnimplementedNodeServer
	service *Service
}

// NodeGetInfo implements csi.NodeServer.
func (n *nodeServer) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: n.service.opts.NodeName}, nil
}

// NodeGetCapabilities implements csi.NodeServer: no staging, stats or expansion.
func (n *nodeServer) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

// NodePublishVolume implements csi.NodeServer: it binds the filesystem's directory at the pod's
// target, read-only if the pod asks. A LOCAL filesystem is first brought to its latest snapshot.
func (n *nodeServer) NodePublishVolume(ctx context.Context, request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	targetPath := request.GetTargetPath()
	if targetPath == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing target path").Err()
	}
	filesystemRn, err := pb.ParseFilesystemRn(request.GetVolumeContext()[filesystemAttribute])
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing filesystem attribute: %v", err).Err()
	}
	filesystem := filesystemRn.String()
	mounted, err := n.service.mounter.Mounted(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	if mounted {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	var sourcePath string
	switch kind := request.GetVolumeContext()[kindAttribute]; kind {
	case kindLocal:
		if sourcePath, err = n.service.prepareLocal(ctx, filesystem); err != nil {
			return nil, status.Errorf(codes.Unavailable, "preparing %s: %v", filesystem, err).Err()
		}
	case kindShared:
		sourcePath = n.service.sharedPath(filesystem)
		if err := os.MkdirAll(sourcePath, 0o755); err != nil {
			return nil, status.Errorf(codes.Internal, "creating %s: %v", filesystem, err).Err()
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown kind %q", kind).Err()
	}
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "creating target: %v", err).Err()
	}
	if err := n.service.mounter.Bind(sourcePath, targetPath, request.GetReadonly()); err != nil {
		return nil, status.Errorf(codes.Internal, "mounting %s: %v", filesystem, err).Err()
	}
	n.service.log.InfoContext(ctx, "mounted filesystem", "filesystem", filesystem, "target", targetPath, "read_only", request.GetReadonly())
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume implements csi.NodeServer: it unbinds the target. A LOCAL copy stays on
// the node for the next mount; stopping its workspace snapshotted it.
func (n *nodeServer) NodeUnpublishVolume(ctx context.Context, request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := request.GetTargetPath()
	if targetPath == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing target path").Err()
	}
	mounted, err := n.service.mounter.Mounted(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	if mounted {
		if err := n.service.mounter.Unmount(targetPath); err != nil {
			return nil, status.Errorf(codes.Internal, "unmounting: %v", err).Err()
		}
	}
	if err := os.Remove(targetPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, status.Errorf(codes.Internal, "removing target: %v", err).Err()
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// mounter binds directories at pods' targets.
type mounter interface {
	// Mounted reports whether something is mounted at target.
	Mounted(target string) (bool, error)
	// Bind mounts source at target, read-only if asked, without setuid or devices.
	Bind(source, target string, readOnly bool) error
	// Unmount unmounts target.
	Unmount(target string) error
}

// bindMounter mounts with the kernel, as the node's root.
type bindMounter struct{}

// Mounted implements mounter by reading the mount table.
func (bindMounter) Mounted(target string) (bool, error) {
	target = filepath.Clean(target)
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, fmt.Errorf("opening mountinfo: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// Field 5 is the mount point, its spaces and the like octal-escaped.
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		if unescapeMountPoint(fields[4]) == target {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("reading mountinfo: %w", err)
	}
	return false, nil
}

// Bind implements mounter. A bind takes its flags only on a remount.
func (bindMounter) Bind(source, target string, readOnly bool) error {
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("binding: %w", err)
	}
	flags := uintptr(unix.MS_BIND | unix.MS_REMOUNT | unix.MS_NOSUID | unix.MS_NODEV)
	if readOnly {
		flags |= unix.MS_RDONLY
	}
	if err := unix.Mount("", target, "", flags, ""); err != nil {
		unix.Unmount(target, unix.MNT_DETACH)
		return fmt.Errorf("remounting: %w", err)
	}
	return nil
}

// Unmount implements mounter.
func (bindMounter) Unmount(target string) error {
	if err := unix.Unmount(target, 0); err != nil && !errors.Is(err, unix.EINVAL) {
		return err
	}
	return nil
}

// unescapeMountPoint undoes mountinfo's octal escapes (`\040` for a space).
func unescapeMountPoint(field string) string {
	if !strings.Contains(field, `\`) {
		return field
	}
	var builder strings.Builder
	for i := 0; i < len(field); i++ {
		if field[i] == '\\' && i+3 < len(field) {
			if value, err := strconv.ParseUint(field[i+1:i+4], 8, 8); err == nil {
				builder.WriteByte(byte(value))
				i += 3
				continue
			}
		}
		builder.WriteByte(field[i])
	}
	return builder.String()
}
