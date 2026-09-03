package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"remount.dev/remount/internal/netns"
	"remount.dev/remount/internal/node"
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
	var list []workspace.Backend
	for _, b := range strings.Split(backends, ",") {
		switch strings.TrimSpace(b) {
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
	return node.New(node.Options{
		DataDir: data, ID: nodeID, Dialer: c.dialer(), Token: c.token, Labels: labels, Backends: workspace.NewRegistry(list...),
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
