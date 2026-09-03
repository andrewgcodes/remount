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
}

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
			if err != nil || !safeLink(rel, filepath.ToSlash(entry.Link)) {
				return result, errors.New("chunked artifact: unsafe symlink")
			}
			entry.Link = filepath.ToSlash(entry.Link)
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
				entry.Chunks, err = snapshotFile(ctx, store, io.LimitReader(file, entry.Size), entry.Size, limits, seenChunks, &result)
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

func snapshotFile(ctx context.Context, store artifact.BlobStore, r io.Reader, expected int64, limits Limits, seen map[string]struct{}, result *SnapshotResult) ([]ChunkRef, error) {
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
		result.Chunks++
		total += int64(len(chunk))
		if _, ok := seen[id]; !ok {
			uploaded, err := putMissing(store, id, chunk)
			if err != nil {
				return nil, err
			}
			seen[id] = struct{}{}
			if uploaded {
				result.ChunksUploaded++
				result.BytesUploaded += int64(len(chunk))
			}
		}
	}
	if total != expected {
		return nil, errors.New("chunked artifact: file changed size during snapshot")
	}
	return refs, nil
}

func putMissing(store artifact.BlobStore, id string, body []byte) (bool, error) {
	size, err := store.Head(id)
	if err == nil {
		if size != int64(len(body)) {
			return false, errors.New("chunked artifact: existing blob has wrong size")
		}
		return false, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
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
