package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/artifact/chunked"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/workspace"
)

// workspaceBlobStore is an operation-scoped, tenant-safe view of the remote
// artifact service. Every remote object operation receives a fresh proof for
// this exact workspace generation. The node's plaintext cache is trusted but
// never establishes tenant existence by itself.
type workspaceBlobStore struct {
	n   *Node
	ctx context.Context
	w   *proto.Workspace
}

func (s *workspaceBlobStore) Put(r io.Reader) (string, int64, error) {
	id, size, err := s.n.store.PutLimit(r, s.n.opts.MaxArtifactBytes)
	if err != nil {
		return "", 0, err
	}
	if err := s.n.upload(s.ctx, s.w, id); err != nil {
		return "", 0, err
	}
	return id, size, nil
}

func (s *workspaceBlobStore) Open(id string) (io.ReadCloser, int64, error) {
	r, err := s.n.fetchWorkspaceArtifact(s.ctx, s.w, id)
	if err != nil {
		return nil, 0, err
	}
	size, err := s.n.store.Head(id)
	if err != nil {
		_ = r.Close()
		return nil, 0, err
	}
	return r, size, nil
}

func (s *workspaceBlobStore) Head(id string) (int64, error) {
	if s.n.opts.ArtifactURL == "" {
		return s.n.store.Head(id)
	}
	req, err := http.NewRequestWithContext(s.ctx, http.MethodHead, strings.TrimSuffix(s.n.opts.ArtifactURL, "/")+"/"+id, nil)
	if err != nil {
		return 0, err
	}
	if err := s.n.authorizeArtifactRequest(s.ctx, s.w, req, id); err != nil {
		return 0, err
	}
	response, err := s.n.opts.HTTPClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return 0, os.ErrNotExist
	}
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("artifact %s: HTTP %d", id, response.StatusCode)
	}
	size, err := strconv.ParseInt(response.Header.Get("Content-Length"), 10, 64)
	if err != nil || size < 0 {
		return 0, fmt.Errorf("artifact %s: invalid Content-Length", id)
	}
	return size, nil
}

func (*workspaceBlobStore) Delete(string) error {
	return errors.New("node: remote artifact deletion is control-plane owned")
}

func (*workspaceBlobStore) List() ([]string, error) {
	return nil, errors.New("node: remote artifact enumeration is unavailable")
}

func (n *Node) chunkedSnapshotEnabled(w *ws) bool {
	n.mu.Lock()
	negotiated := append([]string(nil), n.protocol...)
	n.mu.Unlock()
	if !proto.HasCapability(negotiated, proto.CapabilityChunkedArtifacts) {
		return false
	}
	descriptor, err := n.opts.Backends.Descriptor(w.handle.Backend())
	return err == nil && descriptor.Runtime.Snapshots == "fs"
}

func (n *Node) snapshotChunked(ctx context.Context, w *ws, upload bool, excludes []string) (chunked.SnapshotResult, error) {
	var store artifact.BlobStore = n.store
	if upload {
		if n.opts.ArtifactURL == "" {
			return chunked.SnapshotResult{}, proto.Err(proto.CodeUnsupported, "uploaded chunked snapshots require an artifact endpoint")
		}
		store = &workspaceBlobStore{n: n, ctx: ctx, w: &w.Workspace}
	}
	host, ok := workspace.HostFileSystemOf(w.handle)
	if !ok {
		return chunked.SnapshotResult{}, proto.Err(proto.CodeUnsupported, "backend %s has no host filesystem for chunked snapshot", w.handle.Backend())
	}
	return chunked.Snapshot(ctx, store, host.Root(), chunked.SnapshotOptions{Excludes: excludes})
}

// chunkedTarReader reconstructs the interoperable tar stream directly from
// tenant-authorized chunks. Backend.Create remains the owner of publishing
// its filesystem/runtime, avoiding a root-directory rename underneath a live
// fsops handle or container bind mount.
func (n *Node) chunkedTarReader(ctx context.Context, workspace *proto.Workspace) (io.ReadCloser, <-chan error) {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		store := &workspaceBlobStore{n: n, ctx: ctx, w: workspace}
		err := chunked.ExportTar(ctx, store, workspace.Spec.RestoreFrom, pw, chunked.Limits{})
		_ = pw.CloseWithError(err)
		done <- err
	}()
	return pr, done
}
