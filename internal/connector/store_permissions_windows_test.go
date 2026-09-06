//go:build windows

package connector

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestStoreRefusesWorldReadableWindowsScopeKey(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "scope.key")
	key := make([]byte, connectorKeyBytes)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, key, 0o600); err != nil {
		t.Fatal(err)
	}
	sd, err := windows.SecurityDescriptorFromString("D:P(A;;FR;;;WD)")
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		dacl,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(root, StoreOptions{}); err == nil || !strings.Contains(err.Error(), "unsafe scope key") {
		t.Fatalf("NewStore on a world-readable scope key = %v", err)
	}
}
