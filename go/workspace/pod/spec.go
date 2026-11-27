// Package pod builds, finds and reaches a workspace's pod: the library workspace-gateway,
// workspace-manager and filesystem-manager share, so none carries its own pod logic.
package pod

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	pb "github.com/malonaz/core/genproto/workspace/v1"
	"github.com/malonaz/core/go/grpc"
)

const (
	// LabelName selects every workspace pod, for the reaper and network policies.
	LabelName  = "app.kubernetes.io/name"
	labelValue = "workspace"

	// AnnotationWorkspace holds the workspace's resource name: label values cannot hold it.
	AnnotationWorkspace = "workspace.malonaz.com/workspace"
	// AnnotationLastActive holds when a call last used the pod, RFC 3339.
	AnnotationLastActive = "workspace.malonaz.com/last-active"

	tmpDirectory      = "/tmp"
	containerName     = "workspace-server"
	portName          = "grpc"
	tmpVolume         = "tmp"
	mountVolumePrefix = "mount-"

	// port is the workspace-server's gRPC port.
	port = 9090
	// runtimeClassName sandboxes the commands in gVisor.
	runtimeClassName = "gvisor"
	// csiDriver mounts the filesystems on the node.
	csiDriver = "workspaces.onikisu"
)

var (
	// resources of the workspace-server container, which runs the commands.
	resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("2Gi")},
	}
	// tmpSizeLimit bounds `/tmp`, on the node's disk.
	tmpSizeLimit = resource.MustParse("4Gi")
)

// Config is how every workspace pod runs, the same for all workspaces.
type Config struct {
	// Namespace holds the workspace pods.
	Namespace string
	// Image runs core's workspace-server.
	Image string
	// DisableTLS serves and dials the workspace-server in plaintext.
	DisableTLS bool
	// Env is passed to the workspace-server: TLS and authentication flags.
	Env []corev1.EnvVar
}

// Name is the pod of a workspace: a hash of its resource name, unique across owners.
func Name(workspaceName string) string {
	sum := sha256.Sum256([]byte(workspaceName))
	return "ws-" + hex.EncodeToString(sum[:])[:20]
}

// Spec builds the pod of a workspace with its mounts. filesystemNameToKind holds the kind of
// every mounted filesystem: it decides how the node mounts it.
func Spec(config *Config, workspace *pb.Workspace, filesystemNameToKind map[string]pb.Filesystem_Kind) (*corev1.Pod, error) {
	// Copies: the pod must not alias the shared defaults.
	tmpSizeLimit := tmpSizeLimit.DeepCopy()
	runtimeClassName := runtimeClassName
	volumeMounts := []corev1.VolumeMount{{Name: tmpVolume, MountPath: tmpDirectory}}
	volumes := []corev1.Volume{
		{Name: tmpVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &tmpSizeLimit}}},
	}
	for i, mount := range workspace.GetMounts() {
		kind, ok := filesystemNameToKind[mount.GetFilesystem()]
		if !ok || kind == pb.Filesystem_KIND_UNSPECIFIED {
			return nil, fmt.Errorf("no kind for filesystem %s", mount.GetFilesystem())
		}
		readOnly := mount.GetMode() == pb.Mount_MODE_READ_ONLY
		volume := corev1.Volume{
			Name: mountVolumePrefix + strconv.Itoa(i),
			VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
				Driver:   csiDriver,
				ReadOnly: &readOnly,
				VolumeAttributes: map[string]string{
					"filesystem": mount.GetFilesystem(),
					"kind":       strings.ToLower(strings.TrimPrefix(kind.String(), "KIND_")),
					"readOnly":   strconv.FormatBool(readOnly),
				},
			}},
		}
		volumes = append(volumes, volume)
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: volume.Name, MountPath: mount.GetPath(), ReadOnly: readOnly})
	}

	// Commands start in the first mount, or `/tmp` with none.
	directory := tmpDirectory
	if len(workspace.GetMounts()) > 0 {
		directory = workspace.GetMounts()[0].GetPath()
	}
	env := []corev1.EnvVar{
		{Name: "WORKSPACE_SERVICE_DIRECTORY", Value: directory},
		{Name: "WORKSPACE_SERVICE_GRPC_PORT", Value: strconv.Itoa(port)},
		{Name: "WORKSPACE_SERVICE_GRPC_DISABLE_TLS", Value: strconv.FormatBool(config.DisableTLS)},
	}
	containers := []corev1.Container{{
		Name:         containerName,
		Image:        config.Image,
		Env:          append(env, config.Env...),
		Resources:    *resources.DeepCopy(),
		Ports:        []corev1.ContainerPort{{Name: portName, ContainerPort: port}},
		VolumeMounts: volumeMounts,
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(portName)}},
			PeriodSeconds: 1,
		},
	}}

	automountServiceAccountToken := false
	enableServiceLinks := false
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        Name(workspace.GetName()),
			Namespace:   config.Namespace,
			Labels:      map[string]string{LabelName: labelValue},
			Annotations: map[string]string{AnnotationWorkspace: workspace.GetName()},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName:             &runtimeClassName,
			AutomountServiceAccountToken: &automountServiceAccountToken,
			EnableServiceLinks:           &enableServiceLinks,
			Containers:                   containers,
			Volumes:                      volumes,
		},
	}
	return pod, nil
}

// ClientOpts addresses a running pod's workspace-server by its IP, with the port and TLS its spec
// serves on: pods have no DNS name.
func (c *Config) ClientOpts(pod *corev1.Pod) (*grpc.ClientOpts, error) {
	if pod.Status.PodIP == "" {
		return nil, fmt.Errorf("pod %s has no IP", pod.Name)
	}
	clientOpts := &grpc.ClientOpts{
		Host:       pod.Status.PodIP,
		Port:       port,
		DisableTLS: c.DisableTLS,
	}
	return clientOpts, nil
}
