package chunked

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"remount.dev/remount/internal/artifact"
)

// SnapshotOptions controls one chunked snapshot.
type SnapshotOptions struct {
	Excludes []string
	HotPaths []string
	Limits   Limits
	// Skip drops a path, and a skipped directory's whole subtree, in addition
	// to Excludes. A local push passes localfs's selector here so the chunked
	// and tar representations of one directory hold the same files; the
	// selection rules live in one place rather than being restated per format.
	Skip func(rel string, isDir bool) bool
	// Concurrency bounds how many chunk transfers are in flight. Zero selects
	// DefaultTransferConcurrency; one restores strictly serial transfer.
	Concurrency int
	// MaxUnsharedBytes is the number of new bytes past which this snapshot has
	// to earn chunking rather than be given it. A chunked snapshot pays a whole
	// content-addressed publish, an HTTP request and an authorization per
	// chunk, which costs roughly an order of magnitude more per byte than
	// sending the tree once; it wins only while the bytes the tree does not
	// share with the store are a small fraction of the tree. Past this many
	// unshared bytes, a tree whose unshared share is above
	// one-in-UnsharedShareDivisor is abandoned with ErrNotDeduplicating,
	// before anything is uploaded, so the caller can fall back to a whole-tree
	// format.
	//
	// Zero or negative means no limit, which keeps every chunk transfer
	// immediate and holds no chunk bodies in memory.
	MaxUnsharedBytes int64
}

// UnsharedShareDivisor is the largest share of a tree that may be new before
// chunk-by-chunk transfer stops being cheaper than sending the tree once.
// Measured against this repository's own artifact store, publishing one 64 KiB
// chunk costs about 5 ms even with transfers overlapped, while a whole-tree
// snapshot moves about 150 MB/s, which puts the crossover near one part in
// twelve; one in eight keeps a margin on slower storage.
const UnsharedShareDivisor = 8

// ErrNotDeduplicating reports that a chunked snapshot was abandoned because
// too little of the tree was already stored for chunking to pay for itself.
// No blob was uploaded and no manifest was published.
var ErrNotDeduplicating = errors.New("chunked artifact: snapshot does not deduplicate")

// SnapshotResult reports logical identity and physical deduplication work.
// The tags are load-bearing: `remount push --chunked --json` reports these
// counters, because a deduplicating upload that deduplicated nothing is a
// result the operator needs to see rather than infer.
type SnapshotResult struct {
	ManifestID     string `json:"manifest"`
	ManifestBytes  int64  `json:"manifest_bytes"`
	PlaintextBytes int64  `json:"plaintext_bytes"`
	BytesUploaded  int64  `json:"bytes_uploaded"`
	Chunks         int    `json:"chunks"`
	ChunksUploaded int    `json:"chunks_uploaded"`
}

// Snapshot chunks the filesystem rooted at root, uploads only blobs that Head
// reports missing, then publishes its canonical plaintext manifest.
func Snapshot(ctx context.Context, store artifact.BlobStore, root string, opts SnapshotOptions) (SnapshotResult, error) {
	var result SnapshotResult
	if store == nil {
		return result, errors.New("chunked artifact: blob store is required")
	}
	limits := normalizedLimits(opts.Limits)
	root, err := filepath.Abs(root)
	if err != nil {
		return result, err
	}
	rr, err := os.OpenRoot(root)
	if err != nil {
		return result, err
	}
	defer rr.Close()
	var paths []string
	var pathBytes int64
	err = fs.WalkDir(rr.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if name == "." {
			return nil
		}
		rel := filepath.ToSlash(name)
		if artifact.Excluded(rel, opts.Excludes) || (opts.Skip != nil && opts.Skip(rel, entry.IsDir())) {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		pathBytes += int64(len(rel))
		if len(paths) >= limits.MaxEntries || int64(len(paths)+1)*32+pathBytes > limits.MaxManifestBytes {
			return errors.New("chunked artifact: entry limit exceeded")
		}
		paths = append(paths, rel)
		return nil
	})
	if err != nil {
		return result, err
	}
	sort.Strings(paths)
	manifest := Manifest{
		Version: manifestVersion,
		Chunker: ChunkSpec{Algorithm: "fastcdc-v1", Min: MinChunkSize, Average: AvgChunkSize, Max: MaxChunkSize},
	}
	if len(opts.HotPaths) > limits.MaxEntries {
		return result, errors.New("chunked artifact: hot-set entry limit exceeded")
	}
	seenChunks := make(map[string]struct{})
	// Chunk objects are independent and immutable, so they are published
	// concurrently. Nothing downstream may observe the snapshot before every
	// one of them is durable, which is why the manifest is published only
	// after the pool has been joined below.
	publisher := &chunkPublisher{
		pool: newTransferPool(ctx, opts.Concurrency), store: store,
		unsharedLimit: opts.MaxUnsharedBytes,
	}
	defer publisher.pool.stop()
	var totalXattrBytes int64
	for _, rel := range paths {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		name := filepath.FromSlash(rel)
		info, err := rr.Lstat(name)
		if err != nil {
			return result, err
		}
		entry := Entry{Path: rel, Mode: uint32(info.Mode().Perm()), ModTimeUnix: info.ModTime().UnixNano()}
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			entry.Type = "symlink"
			entry.Link, err = rr.Readlink(name)
			if err == nil {
				entry.Link, err = artifact.PortableSymlinkTarget(rel, entry.Link, root, "")
			}
			if err != nil || !safeLink(rel, entry.Link) {
				return result, errors.New("chunked artifact: unsafe symlink")
			}
		case info.IsDir():
			entry.Type = "dir"
			file, openErr := rr.Open(name)
			if openErr != nil {
				return result, openErr
			}
			entry.Xattrs, err = readXattrs(file, limits)
			closeErr := file.Close()
			if err != nil || closeErr != nil {
				return result, errors.Join(err, closeErr)
			}
		case info.Mode().IsRegular():
			entry.Type = "file"
			file, openErr := rr.Open(name)
			if openErr != nil {
				return result, openErr
			}
			opened, statErr := file.Stat()
			if statErr != nil || !opened.Mode().IsRegular() {
				_ = file.Close()
				return result, errors.New("chunked artifact: file changed type during snapshot")
			}
			entry.Mode = uint32(opened.Mode().Perm())
			entry.ModTimeUnix = opened.ModTime().UnixNano()
			entry.Size = opened.Size()
			if entry.Size < 0 || entry.Size > limits.MaxFileBytes || entry.Size > limits.MaxSnapshotBytes-manifest.TotalBytes {
				_ = file.Close()
				return result, errors.New("chunked artifact: snapshot byte limit exceeded")
			}
			entry.Xattrs, err = readXattrs(file, limits)
			if err == nil {
				entry.Chunks, err = snapshotFile(ctx, publisher, io.LimitReader(file, entry.Size), entry.Size, limits, seenChunks, &result)
			}
			closeErr := file.Close()
			if err != nil || closeErr != nil {
				return result, errors.Join(err, closeErr)
			}
			manifest.TotalBytes += entry.Size
		default:
			return result, fmt.Errorf("chunked artifact: unsupported file type at %q", rel)
		}
		for _, attr := range entry.Xattrs {
			totalXattrBytes += int64(len(attr.Name) + len(attr.Value))
			if totalXattrBytes > limits.MaxTotalXattrBytes {
				return result, errors.New("chunked artifact: extended attributes exceed limit")
			}
		}
		manifest.Entries = append(manifest.Entries, entry)
	}
	// Join every chunk transfer before the manifest exists anywhere. A
	// manifest is a promise that its chunks are fetchable; publishing it while
	// a transfer is outstanding would make that promise ahead of the fact.
	if err := publisher.pool.wait(); err != nil {
		return result, err
	}
	if err := publisher.uploadDeferred(ctx, opts.Concurrency); err != nil {
		return result, err
	}
	// The workers are joined, so their counters are now this goroutine's to
	// read. Nothing before this point may copy them out of the publisher.
	result.ChunksUploaded, result.BytesUploaded = publisher.uploaded()
	manifest.Chunks = result.Chunks
	hot := make(map[string]struct{}, len(opts.HotPaths))
	entryTypes := make(map[string]string, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		entryTypes[entry.Path] = entry.Type
	}
	for _, name := range opts.HotPaths {
		name = filepath.ToSlash(filepath.Clean(name))
		if entryTypes[name] != "file" {
			return result, fmt.Errorf("chunked artifact: hot path %q is not a snapshotted file", name)
		}
		hot[name] = struct{}{}
	}
	for name := range hot {
		manifest.Hot = append(manifest.Hot, name)
	}
	sort.Strings(manifest.Hot)
	body, err := canonicalManifest(manifest, limits)
	if err != nil {
		return result, err
	}
	result.ManifestID = digestID(body)
	result.ManifestBytes = int64(len(body))
	uploaded, err := putMissing(store, result.ManifestID, body)
	if err != nil {
		return result, err
	}
	if uploaded {
		result.BytesUploaded += int64(len(body))
	}
	result.PlaintextBytes = manifest.TotalBytes
	return result, nil
}

// chunkPublisher hands one chunk body to the bounded transfer pool and folds
// each worker's deduplication result back into the shared counters.
type chunkPublisher struct {
	pool          *transferPool
	store         artifact.BlobStore
	unsharedLimit int64

	mu             sync.Mutex
	deferred       []deferredChunk
	unsharedSeen   int64
	plaintextSeen  int64
	uploadedChunks int
	uploadedBytes  int64
}

// observe records one chunk's bytes against the tree total, including chunks
// this snapshot has already seen. The unshared share is judged against the
// tree walked so far, so it is the producer that reports it.
func (p *chunkPublisher) observe(n int) {
	p.mu.Lock()
	p.plaintextSeen += int64(n)
	p.mu.Unlock()
}

// deferredChunk is a chunk the store does not have yet, held until the whole
// tree has been probed. Holding it costs at most unsharedLimit bytes, which is
// the same budget that decides whether this snapshot stays chunked at all.
type deferredChunk struct {
	id   string
	body []byte
}

func (p *chunkPublisher) publish(id string, body []byte) error {
	return p.pool.submit(func(context.Context) error {
		if p.unsharedLimit <= 0 {
			uploaded, err := putMissing(p.store, id, body)
			if err != nil {
				return err
			}
			if uploaded {
				p.record(int64(len(body)))
			}
			return nil
		}
		present, err := blobPresent(p.store, id, len(body))
		if err != nil || present {
			return err
		}
		p.mu.Lock()
		p.unsharedSeen += int64(len(body))
		over := p.unsharedSeen > p.unsharedLimit &&
			p.unsharedSeen*UnsharedShareDivisor > p.plaintextSeen
		if !over {
			p.deferred = append(p.deferred, deferredChunk{id: id, body: body})
		}
		p.mu.Unlock()
		if over {
			return ErrNotDeduplicating
		}
		return nil
	})
}

func (p *chunkPublisher) record(n int64) {
	p.mu.Lock()
	p.uploadedChunks++
	p.uploadedBytes += n
	p.mu.Unlock()
}

// uploaded reports what the workers published. Call it only after joining
// them: a snapshot that returns early must not copy counters a worker may
// still be writing.
func (p *chunkPublisher) uploaded() (int, int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.uploadedChunks, p.uploadedBytes
}

// uploadDeferred publishes the chunks this tree does not share with the store,
// once the whole tree has been probed and the snapshot is known to be worth
// storing as chunks. It must be joined before the manifest is published.
func (p *chunkPublisher) uploadDeferred(ctx context.Context, concurrency int) error {
	p.mu.Lock()
	pending := p.deferred
	p.deferred = nil
	p.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	pool := newTransferPool(ctx, concurrency)
	defer pool.stop()
	for _, chunk := range pending {
		if err := pool.submit(func(context.Context) error {
			uploaded, err := putMissing(p.store, chunk.id, chunk.body)
			if err != nil {
				return err
			}
			if uploaded {
				p.record(int64(len(chunk.body)))
			}
			return nil
		}); err != nil {
			break
		}
	}
	return pool.wait()
}

// blobPresent reports whether the store already holds this exact object.
func blobPresent(store artifact.BlobStore, id string, size int) (bool, error) {
	stored, err := store.Head(id)
	if err == nil {
		if stored != int64(size) {
			return false, errors.New("chunked artifact: existing blob has wrong size")
		}
		return true, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	return false, nil
}

func snapshotFile(ctx context.Context, publisher *chunkPublisher, r io.Reader, expected int64, limits Limits, seen map[string]struct{}, result *SnapshotResult) ([]ChunkRef, error) {
	chunker := NewChunker(r)
	var refs []ChunkRef
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chunk, err := chunker.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		const minimumChunkReferenceBytes = int64(96)
		if len(chunk) == 0 || len(chunk) > MaxChunkSize || result.Chunks >= limits.MaxChunks || int64(result.Chunks+1)*minimumChunkReferenceBytes > limits.MaxManifestBytes {
			return nil, errors.New("chunked artifact: chunk limit exceeded")
		}
		id := digestID(chunk)
		refs = append(refs, ChunkRef{ID: id, Size: len(chunk)})
		publisher.observe(len(chunk))
		result.Chunks++
		total += int64(len(chunk))
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			// The chunker already returned a private copy, so the worker owns
			// these bytes for as long as the transfer runs.
			if err := publisher.publish(id, chunk); err != nil {
				return nil, err
			}
		}
	}
	if total != expected {
		return nil, errors.New("chunked artifact: file changed size during snapshot")
	}
	return refs, nil
}

func putMissing(store artifact.BlobStore, id string, body []byte) (bool, error) {
	present, err := blobPresent(store, id, len(body))
	if err != nil || present {
		return false, err
	}
	got, size, err := store.Put(bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	if got != id || size != int64(len(body)) {
		return false, errors.New("chunked artifact: blob store published wrong identity")
	}
	return true, nil
}

func digestID(body []byte) string {
	sum := sha256.Sum256(body)
	return artifact.ID(sum[:])
}
