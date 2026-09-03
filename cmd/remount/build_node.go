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

	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/workspace"
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
