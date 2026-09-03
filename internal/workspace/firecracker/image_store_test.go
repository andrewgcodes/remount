package firecracker

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func copyClone(source, destination string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	return errors.Join(copyErr, out.Close())
}

func imageFixture(t *testing.T, maxImages int, maxBytes int64, clone func(string, string) error) (*ReflinkStore, string) {
	t.Helper()
	dir := t.TempDir()
	base := filepath.Join(dir, "base.ext4")
	if err := os.WriteFile(base, []byte("base-rootfs"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := newReflinkStore(ReflinkStoreOptions{Dir: filepath.Join(dir, "images"), MaxImages: maxImages, MaxLogicalBytes: maxBytes, OwnerUID: os.Getuid(), OwnerGID: os.Getgid()}, clone)
	if err != nil {
		t.Fatal(err)
	}
	return store, base
}

func TestReflinkStoreAtomicCapacityAndDestroyHandoff(t *testing.T) {
	store, base := imageFixture(t, 1, 64, copyClone)
	image, err := store.Create(context.Background(), "ws_one", base)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(image.Path()); err != nil || string(got) != "base-rootfs" {
		t.Fatalf("image=%q err=%v", got, err)
	}
	if _, err := store.Create(context.Background(), "ws_two", base); err == nil || !strings.Contains(err.Error(), "capacity") {
		t.Fatalf("over-capacity err=%v", err)
	}
	if err := image.Destroy(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(context.Background(), "ws_two", base); err != nil {
		t.Fatalf("capacity was not handed off after durable removal: %v", err)
	}
}

func TestReflinkStoreReconstructsAccountingAndCleansOwnedStaging(t *testing.T) {
	store, base := imageFixture(t, 2, 64, copyClone)
	image, err := store.Create(context.Background(), "ws_one", base)
	if err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(store.dir, ".image-stage-crash")
	if err := os.WriteFile(stage, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted, err := newReflinkStore(ReflinkStoreOptions{Dir: store.dir, MaxImages: 2, MaxLogicalBytes: 64, OwnerUID: os.Getuid(), OwnerGID: os.Getgid()}, copyClone)
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := restarted.Adopt("ws_one")
	if err != nil {
		t.Fatal(err)
	}
	if adopted.Path() != image.Path() || adopted.Size() != int64(len("base-rootfs")) {
		t.Fatalf("adopted=%+v original=%+v", adopted, image)
	}
	if _, err := os.Stat(stage); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned staging path survived reconstruction: %v", err)
	}
}

func TestReflinkStoreConcurrentReservationPreventsOvercommit(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	clone := func(source, destination string) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return copyClone(source, destination)
	}
	store, base := imageFixture(t, 1, 64, clone)
	done := make(chan error, 1)
	go func() {
		_, err := store.Create(context.Background(), "ws_one", base)
		done <- err
	}()
	<-entered
	if _, err := store.Create(context.Background(), "ws_two", base); err == nil || !strings.Contains(err.Error(), "capacity") {
		t.Fatalf("concurrent admission err=%v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestReflinkStoreFailedCloneReleasesReservation(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	clone := func(source, destination string) error {
		if fail.Swap(false) {
			return errors.New("injected clone failure")
		}
		return copyClone(source, destination)
	}
	store, base := imageFixture(t, 1, 64, clone)
	if _, err := store.Create(context.Background(), "ws_one", base); err == nil {
		t.Fatal("clone unexpectedly succeeded")
	}
	if _, err := store.Create(context.Background(), "ws_two", base); err != nil {
		t.Fatalf("failed clone retained admission: %v", err)
	}
}

func TestReflinkStoreRejectsSymlinkAndUnknownDurableObject(t *testing.T) {
	store, base := imageFixture(t, 2, 64, copyClone)
	link := filepath.Join(filepath.Dir(base), "base-link")
	if err := os.Symlink(base, link); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(context.Background(), "ws_one", link); err == nil {
		t.Fatal("symlink base accepted")
	}
	if err := os.WriteFile(filepath.Join(store.dir, "unknown"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newReflinkStore(ReflinkStoreOptions{Dir: store.dir, OwnerUID: os.Getuid(), OwnerGID: os.Getgid()}, copyClone); err == nil || !strings.Contains(err.Error(), "unrecognized") {
		t.Fatalf("unknown durable object err=%v", err)
	}
}

func FuzzImageNameRoundTrip(f *testing.F) {
	for _, seed := range []string{"ws_one", "A-b.c_1", "x"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, id string) {
		if validateWorkspaceID(id) != nil {
			return
		}
		got, ok := parseImageName(imageName(id))
		if !ok || got != id {
			t.Fatalf("round trip %q => %q ok=%v", id, got, ok)
		}
	})
}
