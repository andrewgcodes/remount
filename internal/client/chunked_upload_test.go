package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/artifact/chunked"
)

// artifactService is the subset of /v1/artifacts a chunked upload uses. It
// counts methods because deduplication is a claim about which requests are
// made, not only about the numbers reported afterwards.
type artifactService struct {
	mu     sync.Mutex
	blobs  map[string][]byte
	counts map[string]int
}

func newArtifactService(t *testing.T) (*artifactService, string) {
	t.Helper()
	s := &artifactService{blobs: map[string][]byte{}, counts: map[string]int{}}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	return s, srv.URL
}

func (s *artifactService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/")
	s.mu.Lock()
	s.counts[r.Method]++
	body, held := s.blobs[id]
	s.mu.Unlock()
	switch r.Method {
	case http.MethodPut:
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sum := sha256.Sum256(payload)
		if artifact.ID(sum[:]) != id {
			http.Error(w, "digest mismatch", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.blobs[id] = payload
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	case http.MethodHead, http.MethodGet:
		if !held {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *artifactService) method(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[name]
}

// TestHeadArtifactReportsAbsenceWithoutTransferringIt pins the property that
// makes chunk deduplication worth doing. Answering "do you already have this?"
// with a GET would download every chunk in order to learn it was redundant.
func TestHeadArtifactReportsAbsenceWithoutTransferringIt(t *testing.T) {
	service, url := newArtifactService(t)
	c := New(Options{ArtifactURL: url, Token: "t"})
	ctx := context.Background()

	body := []byte("stored artifact")
	sum := sha256.Sum256(body)
	id := artifact.ID(sum[:])
	if _, err := c.HeadArtifact(ctx, id); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("head of an absent artifact = %v, want fs.ErrNotExist", err)
	}
	if _, _, err := c.UploadArtifact(ctx, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	size, err := c.HeadArtifact(ctx, id)
	if err != nil || size != int64(len(body)) {
		t.Fatalf("head = %d, %v", size, err)
	}
	if got := service.method(http.MethodGet); got != 0 {
		t.Fatalf("head issued %d GET requests", got)
	}
	if _, err := c.HeadArtifact(ctx, "not-an-artifact-id"); err == nil {
		t.Fatal("head accepted a malformed id")
	}
}

// TestUploadChunkedSnapshotTransfersOnlyNewContent is the deduplication proof.
// A second push of a mostly-unchanged tree must move a small fraction of the
// bytes, and must reach the same manifest for an unchanged tree.
func TestUploadChunkedSnapshotTransfersOnlyNewContent(t *testing.T) {
	service, url := newArtifactService(t)
	c := New(Options{ArtifactURL: url, Token: "t"})
	ctx := context.Background()

	dir := t.TempDir()
	bulk := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog\n"), 40_000)
	if err := os.WriteFile(filepath.Join(dir, "bulk.txt"), bulk, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "small.txt"), []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}

	first, err := c.UploadChunkedSnapshot(ctx, dir, chunked.SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Chunks < 2 || first.ChunksUploaded != first.Chunks {
		t.Fatalf("first upload = %+v, want every chunk transferred", first)
	}
	if first.BytesUploaded < int64(len(bulk)) {
		t.Fatalf("first upload moved %d bytes, want at least the tree's %d", first.BytesUploaded, len(bulk))
	}

	// An identical tree republishes the same manifest and sends nothing.
	repeat, err := c.UploadChunkedSnapshot(ctx, dir, chunked.SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if repeat.ManifestID != first.ManifestID || repeat.ChunksUploaded != 0 || repeat.BytesUploaded != 0 {
		t.Fatalf("unchanged tree = %+v, want the same manifest and no transfer", repeat)
	}

	if err := os.WriteFile(filepath.Join(dir, "small.txt"), []byte("v2"), 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := c.UploadChunkedSnapshot(ctx, dir, chunked.SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if second.ManifestID == first.ManifestID {
		t.Fatal("changed tree produced the previous manifest")
	}
	if second.Chunks != first.Chunks {
		t.Fatalf("chunk count changed unexpectedly: %d -> %d", first.Chunks, second.Chunks)
	}
	if second.ChunksUploaded != 1 {
		t.Fatalf("second upload sent %d of %d chunks, want only the changed one", second.ChunksUploaded, second.Chunks)
	}
	if second.BytesUploaded*20 > first.BytesUploaded {
		t.Fatalf("second upload moved %d bytes against the first's %d; deduplication did not happen",
			second.BytesUploaded, first.BytesUploaded)
	}
	if got := service.method(http.MethodGet); got != 0 {
		t.Fatalf("a chunked push downloaded %d objects; existence must come from HEAD", got)
	}
}

// TestUploadChunkedSnapshotRequiresAnArtifactEndpoint keeps the capability
// check ahead of the work, so a client with nowhere to publish fails before
// hashing a tree rather than after.
func TestUploadChunkedSnapshotRequiresAnArtifactEndpoint(t *testing.T) {
	c := New(Options{})
	if _, err := c.UploadChunkedSnapshot(context.Background(), t.TempDir(), chunked.SnapshotOptions{}); err != ErrNoArtifactURL {
		t.Fatalf("error = %v, want ErrNoArtifactURL", err)
	}
	if _, err := c.HeadArtifact(context.Background(), "art_sha256:00"); err != ErrNoArtifactURL {
		t.Fatalf("error = %v, want ErrNoArtifactURL", err)
	}
}
