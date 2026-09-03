package volume

import (
	"context"
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

func TestDirectoryResolverRejectsEverySymlinkComponent(t *testing.T) {
	root := t.TempDir()
	id := artifactID('a')
	regular := filepath.Join(root, "regular", id)
	if err := os.MkdirAll(regular, 0o700); err != nil {
		t.Fatal(err)
	}
	resolver, err := OpenDirectoryResolver(root)
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()
	f, err := resolver.OpenArtifact(context.Background(), "regular", id)
	if err != nil {
		t.Fatalf("regular artifact: %v", err)
	}
	f.Close()

	if err := os.Symlink("regular", filepath.Join(root, "tenant-link")); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.OpenArtifact(context.Background(), "tenant-link", id); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("tenant symlink = %v, want unsafe path", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "same-tenant", "real"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real", filepath.Join(root, "same-tenant", id)); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.OpenArtifact(context.Background(), "same-tenant", id); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("internal artifact symlink = %v, want unsafe path", err)
	}
	if err := os.Mkdir(filepath.Join(root, "other-tenant"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..", "regular", id), filepath.Join(root, "other-tenant", id)); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.OpenArtifact(context.Background(), "other-tenant", id); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("cross-tenant artifact symlink = %v, want unsafe path", err)
	}
}

func TestDirectoryResolverAcceptsTenantIdentityGrammar(t *testing.T) {
	root := t.TempDir()
	tenant, id := "org:team@example.com", artifactID('b')
	if err := os.MkdirAll(filepath.Join(root, tenant, id), 0o700); err != nil {
		t.Fatal(err)
	}
	resolver, err := OpenDirectoryResolver(root)
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()
	f, err := resolver.OpenArtifact(context.Background(), tenant, id)
	if err != nil {
		t.Fatalf("safe tenant identity was rejected: %v", err)
	}
	f.Close()
	for _, invalid := range []string{".", "..", "tenant/name", "tenant\\name"} {
		if err := ValidateTenant(invalid); err == nil {
			t.Errorf("unsafe tenant %q accepted", invalid)
		}
	}
}

func FuzzCleanMountPath(f *testing.F) {
	for _, seed := range []string{"/data", "/cache/packages", "../escape", "/a/../b", "/.remount/env", "/a//b", "", "/"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, candidate string) {
		clean, rel, err := cleanMountPath(candidate)
		if err != nil {
			return
		}
		if clean != candidate || clean != path.Clean(candidate) || !strings.HasPrefix(clean, "/") {
			t.Fatalf("accepted non-canonical path %q as %q", candidate, clean)
		}
		if rel == "" || rel == ".remount" || strings.HasPrefix(rel, ".remount/") {
			t.Fatalf("accepted reserved relative path %q", rel)
		}
		for _, part := range strings.Split(rel, "/") {
			if part == "" || part == "." || part == ".." {
				t.Fatalf("accepted unsafe component %q in %q", part, rel)
			}
		}
	})
}
