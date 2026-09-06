package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"remount.dev/remount/internal/netns"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/profile"
	"remount.dev/remount/internal/workspace"
	"remount.dev/remount/internal/workspace/firecracker"
	"remount.dev/remount/internal/workspace/gvisor"
)

type nodeResourceOptions struct {
	artifactBytes             int64
	artifactStoreBytes        int64
	artifactObjects           int
	artifactRetention         time.Duration
	artifactGCInterval        time.Duration
	connectorCacheBytes       int64
	connectorWorkspaceBytes   int64
	connectorObjectBytes      int64
	connectorObjects          int64
	connectorWorkspaceObjects int64
	maxSessions               int
	maxActiveSessions         int
	maxWorkspaceSessions      int
	maxPrincipalSessions      int
	sessionMemoryBytes        int
	sessionSpillBytes         int64
	sessionMaxChunkBytes      int
	sessionMemoryChunks       int
	maxConcurrentRequests     int
	mutationRetention         time.Duration
	maxMutationRecords        int
	maxConcurrentSnapshots    int
	snapshotMinInterval       time.Duration
	firecracker               firecrackerNodeOptions
	// profile is the runtime profile this node claims (ADR 0089). It lives
	// here rather than in buildNode's parameter list so adding it did not
	// rewrite every call site. Empty is profile.Dev.
	profile string
	// profileHealthInterval overrides the drift-probe cadence; zero selects
	// node.DefaultProfileHealthInterval.
	profileHealthInterval time.Duration
}

type firecrackerNodeOptions struct {
	kernel, rootfs, binary, jailer, guestManifest, chroot string
	uid, gid, maxWorkspaces, maxImages                    int
	imageBytes                                            int64
	cgroupVersion                                         int
	cgroupParent                                          string
	cgroups                                               []string
}

func buildNode(data, nodeID string, c common, labels map[string]string, backends, image string, allow, allowPrivate []string, resources nodeResourceOptions) (*node.Node, error) {
	if !filepath.IsAbs(data) {
		return nil, fmt.Errorf("node data directory %q must be absolute", data)
	}
	runtimeProfile, err := profile.Parse(resources.profile)
	if err != nil {
		return nil, err
	}
	var list []workspace.Backend
	for _, b := range strings.Split(backends, ",") {
		name := strings.TrimSpace(b)
		// A profile that promises isolation between mutually untrusted
		// workloads refuses the shared-kernel development backends outright,
		// before construction. Leaving them out of the registry is what makes
		// the refusal unbypassable: a backend that was never registered
		// cannot be selected by a workspace, a label or a later drift.
		if runtimeProfile.Rank() >= profile.MultiTenantIsolated.Rank() && (name == "process" || name == "docker") {
			return nil, fmt.Errorf("runtime profile %s refuses the %s backend: it shares the host kernel", runtimeProfile, name)
		}
		switch name {
		case "process":
			pb, err := workspace.NewProcess(filepath.Join(data, "ws"))
			if err != nil {
				return nil, err
			}
			list = append(list, pb)
		case "docker":
			d, err := workspace.NewDocker(filepath.Join(data, "ws"), image)
			if err != nil {
				return nil, err
			}
			list = append(list, d)
		case "gvisor":
			rootfs := os.Getenv("REMOUNT_GVISOR_ROOTFS")
			if rootfs == "" {
				return nil, errors.New("gvisor backend requires REMOUNT_GVISOR_ROOTFS to name an unpacked rootfs")
			}
			probeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			g, err := gvisor.New(probeCtx, gvisor.Options{
				Dir: filepath.Join(data, "ws-gvisor"), RootFS: rootfs, Runsc: os.Getenv("REMOUNT_RUNSC"),
			})
			cancel()
			if err != nil {
				return nil, fmt.Errorf("gvisor backend unavailable: %w", err)
			}
			list = append(list, g)
		case "firecracker":
			fc := resources.firecracker
			if fc.kernel == "" || fc.rootfs == "" || fc.guestManifest == "" {
				return nil, errors.New("firecracker backend requires --firecracker-kernel, --firecracker-rootfs, and --firecracker-guest-manifest")
			}
			factory, err := firecracker.NewJailerFactory(firecracker.JailerOptions{
				Firecracker: fc.binary, Jailer: fc.jailer, ChrootBase: fc.chroot, UID: fc.uid, GID: fc.gid,
				CgroupVersion: fc.cgroupVersion, CgroupParent: fc.cgroupParent, Cgroups: fc.cgroups,
				MaxMachines: fc.maxWorkspaces,
			})
			if err != nil {
				return nil, fmt.Errorf("firecracker jailer: %w", err)
			}
			images, err := firecracker.NewReflinkStore(firecracker.ReflinkStoreOptions{
				Dir: filepath.Join(data, "firecracker", "images"), MaxImages: fc.maxImages,
				MaxLogicalBytes: fc.imageBytes, OwnerUID: fc.uid, OwnerGID: fc.gid,
			})
			if err != nil {
				return nil, fmt.Errorf("firecracker images: %w", err)
			}
			networks, err := firecracker.NewSystemNetworkProvider(context.Background(), firecracker.NetworkOptions{
				Dir: filepath.Join(data, "firecracker", "networks"), Manager: netns.NewSystemManager(), UID: fc.uid, GID: fc.gid,
			})
			if err != nil {
				return nil, fmt.Errorf("firecracker network: %w", err)
			}
			guest, err := firecracker.NewGuestBridge(firecracker.GuestOptions{Manifest: fc.guestManifest})
			if err != nil {
				return nil, fmt.Errorf("firecracker guest: %w", err)
			}
			binary := fc.binary
			if binary == "" {
				binary = "firecracker"
			}
			volumes, err := firecracker.NewCoWVolumeProvider(firecracker.CoWVolumeOptions{
				Dir: filepath.Join(data, "firecracker", "snapshots"), Images: images, Firecracker: binary,
			})
			if err != nil {
				return nil, fmt.Errorf("firecracker volumes: %w", err)
			}
			probeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			backend, err := firecracker.New(probeCtx, firecracker.Options{
				Dir: filepath.Join(data, "firecracker"), KernelImage: fc.kernel, BaseRootFS: fc.rootfs,
				Machines: factory, Networks: networks, Volumes: volumes, Guest: guest, MaxWorkspaces: fc.maxWorkspaces,
			})
			cancel()
			if err != nil {
				return nil, fmt.Errorf("firecracker backend unavailable: %w", err)
			}
			list = append(list, backend)
		case "":
		default:
			return nil, fmt.Errorf("unknown backend %q", b)
		}
	}
	if len(list) == 0 {
		return nil, errors.New("no backends")
	}
	registry := workspace.NewRegistry(list...)
	if err := gateRuntimeProfile(runtimeProfile, registry); err != nil {
		return nil, err
	}
	return node.New(node.Options{
		DataDir: data, ID: nodeID, Dialer: c.dialer(), Token: c.token, Labels: labels, Backends: registry,
		Profile: string(runtimeProfile), ProfileHealthInterval: resources.profileHealthInterval,
		ArtifactURL: strings.TrimSuffix(c.server, "/") + "/v1/artifacts", Logger: slog.Default(),
		MaxArtifactBytes: resources.artifactBytes, MaxArtifactStoreBytes: resources.artifactStoreBytes,
		MaxArtifactObjects: resources.artifactObjects,
		ArtifactRetention:  resources.artifactRetention, ArtifactGCInterval: resources.artifactGCInterval,
		MaxConnectorCacheBytes:       resources.connectorCacheBytes,
		MaxConnectorWorkspaceBytes:   resources.connectorWorkspaceBytes,
		MaxConnectorObjectBytes:      resources.connectorObjectBytes,
		MaxConnectorObjects:          resources.connectorObjects,
		MaxConnectorWorkspaceObjects: resources.connectorWorkspaceObjects,
		MaxSessions:                  resources.maxSessions, MaxActiveSessions: resources.maxActiveSessions,
		MaxSessionsPerWorkspace: resources.maxWorkspaceSessions,
		MaxSessionsPerPrincipal: resources.maxPrincipalSessions,
		SessionMemoryBytes:      resources.sessionMemoryBytes, SessionSpillBytes: resources.sessionSpillBytes,
		SessionMaxChunkBytes: resources.sessionMaxChunkBytes, SessionMaxMemoryChunks: resources.sessionMemoryChunks,
		MaxConcurrentRequests: resources.maxConcurrentRequests,
		MutationRetention:     resources.mutationRetention, MaxMutationRecords: resources.maxMutationRecords,
		MaxConcurrentSnapshots: resources.maxConcurrentSnapshots, SnapshotMinInterval: resources.snapshotMinInterval,
		Allow: allow, AllowPrivate: allowPrivate, Version: version,
	})
}

// gateRuntimeProfile is the node's fail-closed startup gate (ADR 0089).
//
// It runs after every backend has been constructed, so it evaluates exactly
// the evidence the node is about to advertise: a backend whose constructor
// failed is not in the registry, and a backend that could not verify its own
// prerequisites advertises a zero descriptor. It also runs one drift probe,
// because a host that is already degraded must not serve for a whole probe
// interval before anyone notices.
//
// A profile that is not fully satisfied is refused. The report goes to stderr
// as JSON so an operator or a provisioning script reads the exact failing
// check rather than a sentence.
func gateRuntimeProfile(p profile.Profile, registry *workspace.Registry) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	checks := workspace.ReprobeRegistry(ctx, registry)
	cancel()
	report := profile.Evaluate(p, registry.Descriptors(), checks)
	if report.OK() {
		return nil
	}
	body, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		fmt.Fprintln(os.Stderr, string(body))
	}
	return fmt.Errorf("runtime profile %s is not satisfied (%s): %s",
		p, report.Status, strings.Join(report.Failed(), ", "))
}
