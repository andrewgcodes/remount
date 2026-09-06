package connector

import (
	"os"
	"path/filepath"
	"testing"

	"remount.dev/remount/internal/privatefile"
)

func TestStoreCreatesPrivateScopeKey(t *testing.T) {
	root := t.TempDir()
	if _, err := NewStore(root, StoreOptions{}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "scope.key")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := privatefile.ValidateFile(path, info); err != nil {
		t.Fatalf("scope key permissions: %v", err)
	}
}
