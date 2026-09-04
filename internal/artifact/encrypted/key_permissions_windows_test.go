//go:build windows

package encrypted

import (
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileMasterKeyRejectsWindowsEveryoneReadACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "master.key")
	body := base64.StdEncoding.EncodeToString(make([]byte, 32))
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("icacls.exe", path, "/grant", "*S-1-1-0:(R)").CombinedOutput(); err != nil {
		t.Fatalf("grant Everyone read: %v\n%s", err, out)
	}
	if _, err := NewFileMasterKey(path, "v1"); err == nil || !strings.Contains(err.Error(), "must not be group or world accessible") {
		t.Fatalf("NewFileMasterKey error = %v", err)
	}
}
