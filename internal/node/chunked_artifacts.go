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
	"sync"

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

// chunkedUnsharedLimit is the number of new bytes past which this node stops
// storing a snapshot as chunks. Chunk transfer costs a content-addressed
// publish, an HTTP request and an authorization per 64 KiB object, so it only
// wins when most of the tree is already stored; past this point the whole-tree
// format sends the same bytes in one object. Zero selects the default.
func (n *Node) chunkedUnsharedLimit() int64 {
	if n.opts.MaxChunkedUnsharedBytes < 0 {
		return 0
	}
	if n.opts.MaxChunkedUnsharedBytes == 0 {
		return DefaultMaxChunkedUnsharedBytes
	}
	return n.opts.MaxChunkedUnsharedBytes
}

// DefaultMaxChunkedUnsharedBytes is the default chunkedUnsharedLimit: the
// number of new bytes below which chunking is simply allowed, because at that
// size the per-chunk cost is too small to be worth reasoning about. Above it,
// a snapshot keeps chunking only while its new bytes stay under
// one-in-chunked.UnsharedShareDivisor of the tree, so an ordinary incremental
// checkpoint still deduplicates and a workspace full of new content does not
// pay per-chunk transfer for all of it.
const DefaultMaxChunkedUnsharedBytes = 8 << 20

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
	return chunked.Snapshot(ctx, store, host.Root(), chunked.SnapshotOptions{
		Excludes: excludes, MaxUnsharedBytes: n.chunkedUnsharedLimit(),
	})
}

// chunkedOverlayArchive reconstructs the interoperable tar stream for a
// chunked manifest so an overlay can reuse the single validated apply path.
// Every blob is fetched and digest-verified into the local store before the
// stream is offered, matching the tar path where the whole download also
// finishes before the caller takes the workspace tree boundary.
func (n *Node) chunkedOverlayArchive(ctx context.Context, w *proto.Workspace, id string) (io.ReadCloser, error) {
	job, err := chunked.StartPrefetch(ctx, &workspaceBlobStore{n: n, ctx: ctx, w: w}, n.store, id, chunked.Limits{})
	if err != nil {
		return nil, err
	}
	if err := job.Wait(); err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	archive := &joinedArchive{PipeReader: pr, done: make(chan error, 1)}
	go func() {
		err := chunked.ExportTar(ctx, n.store, id, pw, chunked.Limits{})
		_ = pw.CloseWithError(err)
		archive.done <- err
	}()
	return archive, nil
}

// joinedArchive couples a reconstructed stream to its producer so that Close
// is evidence the producer exited, not a request that it stop.
type joinedArchive struct {
	*io.PipeReader
	once sync.Once
	done chan error
	err  error
}

func (a *joinedArchive) Close() error {
	a.once.Do(func() {
		_ = a.PipeReader.Close()
		a.err = <-a.done
		if errors.Is(a.err, io.ErrClosedPipe) {
			// The overlay stops at the tar end-of-archive marker and never
			// drains the gzip trailer, so a producer parked on that last
			// write has finished its work. A genuine production failure
			// reaches the reader through CloseWithError instead.
			a.err = nil
		}
	})
	return a.err
}

// chunkedTarReader reconstructs the interoperable tar stream directly from
// tenant-authorized chunks. Backend.Create remains the owner of publishing
// its filesystem/runtime, avoiding a root-directory rename underneath a live
// fsops handle or container bind mount.
func (n *Node) chunkedTarReader(ctx context.Context, workspace *proto.Workspace) (io.ReadCloser, <-chan error) {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		// Every chunk is authorized and digest-verified into this node's cache
		// first, then the tar stream is reconstructed from the cache. Exporting
		// straight from the remote store would issue one authorized download
		// per 64 KiB chunk in tar order, and each of those pays a full
		// content-addressed publish; fetching them concurrently is the whole
		// difference between a few MB/s and the link's speed.
		err := n.cacheChunkedArtifact(ctx, workspace, workspace.Spec.RestoreFrom)
		if err == nil {
			err = chunked.ExportTar(ctx, n.store, workspace.Spec.RestoreFrom, pw, chunked.Limits{})
		}
		_ = pw.CloseWithError(err)
		done <- err
	}()
	return pr, done
}

// cacheChunkedArtifact authorizes and downloads a manifest and every distinct
// chunk it references into this node's plaintext cache, with bounded
// concurrency. Each chunk still travels through the same per-workspace,
// per-generation artifact proof it would have used one at a time, so what
// changes is how many are in flight, never who may fetch which bytes.
func (n *Node) cacheChunkedArtifact(ctx context.Context, w *proto.Workspace, manifestID string) error {
	store := &workspaceBlobStore{n: n, ctx: ctx, w: w}
	manifest, err := chunked.LoadManifest(store, manifestID, chunked.Limits{})
	if err != nil {
		return err
	}
	seen := make(map[string]struct{})
	ids := make([]string, 0, manifest.Chunks)
	for _, entry := range manifest.Entries {
		for _, chunk := range entry.Chunks {
			if _, duplicate := seen[chunk.ID]; duplicate {
				continue
			}
			seen[chunk.ID] = struct{}{}
			ids = append(ids, chunk.ID)
		}
	}
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	inFlight := make(chan struct{}, chunked.DefaultTransferConcurrency)
	var wait sync.WaitGroup
	var mu sync.Mutex
	var terminal error
	for _, id := range ids {
		select {
		case inFlight <- struct{}{}:
		case <-fetchCtx.Done():
		}
		if fetchCtx.Err() != nil {
			break
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			defer func() { <-inFlight }()
			r, err := n.fetchWorkspaceArtifact(fetchCtx, w, id)
			if err != nil {
				err = fmt.Errorf("fetch chunk %s: %w", id, err)
			} else {
				err = r.Close()
			}
			if err == nil {
				return
			}
			mu.Lock()
			if terminal == nil {
				terminal = err
				// One unfetchable chunk makes the whole restore impossible.
				// Cancelling stops the remaining downloads rather than paying
				// for an artifact that can never be complete.
				cancel()
			}
			mu.Unlock()
		}()
	}
	// Waiting joins every fetch, so a nil error is evidence the cache holds
	// the whole artifact rather than a request that it should.
	wait.Wait()
	mu.Lock()
	defer mu.Unlock()
	if terminal != nil {
		return terminal
	}
	return ctx.Err()
}
