package chunked

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"sync"

	"remount.dev/remount/internal/artifact"
)

// PrefetchJob owns one background lazy-prefetch operation. Cancel requests
// termination; only Wait proves the worker exited and reports its final error.
type PrefetchJob struct {
	manifest Manifest
	cancel   context.CancelFunc
	done     chan struct{}

	mu  sync.Mutex
	err error
}

// Manifest returns the already verified manifest whose hot set was fetched
// before StartPrefetch returned.
func (j *PrefetchJob) Manifest() Manifest { return j.manifest }

// Cancel requests that background fetching stop. Call Wait to join it.
func (j *PrefetchJob) Cancel() { j.cancel() }

// Wait joins background fetching and returns its terminal result.
func (j *PrefetchJob) Wait() error {
	<-j.done
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.err
}

// StartPrefetch verifies the remote manifest, copies it and every hot-set
// chunk into cache synchronously, then fetches the remaining chunks with a
// bounded pool of background workers. A caller publishes or restores the full
// tree only after Wait succeeds; no incomplete filesystem is presented as
// restored.
func StartPrefetch(ctx context.Context, remote, cache artifact.BlobStore, manifestID string, limits Limits) (*PrefetchJob, error) {
	if remote == nil || cache == nil {
		return nil, errors.New("chunked artifact: remote and cache stores are required")
	}
	limits = normalizedLimits(limits)
	r, size, err := remote.Open(manifestID)
	if err != nil {
		return nil, err
	}
	if size > limits.MaxManifestBytes {
		_ = r.Close()
		return nil, errors.New("chunked artifact: manifest exceeds limit")
	}
	manifest, body, decodeErr := decodeManifest(r, manifestID, limits)
	closeErr := r.Close()
	if decodeErr != nil || closeErr != nil {
		return nil, errors.Join(decodeErr, closeErr)
	}
	if _, err := putMissing(cache, manifestID, body); err != nil {
		return nil, err
	}
	hotPaths := make(map[string]struct{}, len(manifest.Hot))
	for _, path := range manifest.Hot {
		hotPaths[path] = struct{}{}
	}
	hotChunks := make(map[string]struct{})
	var remaining []ChunkRef
	seen := make(map[string]struct{})
	for _, entry := range manifest.Entries {
		_, hot := hotPaths[entry.Path]
		for _, chunk := range entry.Chunks {
			if _, duplicate := seen[chunk.ID]; duplicate {
				if hot {
					hotChunks[chunk.ID] = struct{}{}
				}
				continue
			}
			seen[chunk.ID] = struct{}{}
			if hot {
				hotChunks[chunk.ID] = struct{}{}
			} else {
				remaining = append(remaining, chunk)
			}
		}
	}
	// A chunk shared by a hot and cold file is fetched synchronously and removed
	// from the background set regardless of which file appeared first.
	filtered := remaining[:0]
	for _, chunk := range remaining {
		if _, hot := hotChunks[chunk.ID]; !hot {
			filtered = append(filtered, chunk)
		}
	}
	remaining = filtered
	for _, entry := range manifest.Entries {
		if _, hot := hotPaths[entry.Path]; !hot {
			continue
		}
		for _, chunk := range entry.Chunks {
			if _, first := hotChunks[chunk.ID]; !first {
				continue
			}
			if err := copyBlob(ctx, remote, cache, chunk); err != nil {
				return nil, err
			}
			delete(hotChunks, chunk.ID)
		}
	}
	background, cancel := context.WithCancel(ctx)
	job := &PrefetchJob{manifest: manifest, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(job.done)
		defer cancel()
		// Cold chunks are independent objects, so they are fetched with
		// bounded concurrency. Wait still joins every worker, so a caller that
		// sees Wait return has evidence the cache is complete or the reason it
		// is not.
		pool := newTransferPool(background, limits.Concurrency)
		defer pool.stop()
		for _, chunk := range remaining {
			if err := pool.submit(func(ctx context.Context) error {
				return copyBlob(ctx, remote, cache, chunk)
			}); err != nil {
				break
			}
		}
		terminal := pool.wait()
		job.mu.Lock()
		job.err = terminal
		job.mu.Unlock()
	}()
	return job, nil
}

func copyBlob(ctx context.Context, source, destination artifact.BlobStore, chunk ChunkRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if size, err := destination.Head(chunk.ID); err == nil {
		if size != int64(chunk.Size) {
			return errors.New("chunked artifact: cached chunk has wrong size")
		}
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	r, size, err := source.Open(chunk.ID)
	if err != nil {
		return err
	}
	if size != int64(chunk.Size) {
		_ = r.Close()
		return errors.New("chunked artifact: source chunk has wrong size")
	}
	body, readErr := io.ReadAll(io.LimitReader(&ctxReader{ctx: ctx, r: r}, int64(chunk.Size)+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil {
		return errors.Join(readErr, closeErr)
	}
	defer clear(body)
	if len(body) != chunk.Size || digestID(body) != chunk.ID {
		return errors.New("chunked artifact: source chunk failed verification")
	}
	got, written, err := destination.Put(bytes.NewReader(body))
	if err != nil {
		return err
	}
	if got != chunk.ID || written != int64(chunk.Size) {
		return errors.New("chunked artifact: cache published wrong chunk")
	}
	return nil
}
