//go:build windows

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestReadSecretFileRefusesWorldReadableWindowsDACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "open.token")
	if err := os.WriteFile(path, []byte("enroll_abc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setWorldReadableWindowsDACL(t, path)
	if _, err := readSecretFile(path); err == nil || !strings.Contains(err.Error(), "another Windows principal") {
		t.Fatalf("readSecretFile on a world-readable DACL = %v", err)
	}
}

func TestStoredCredentialRefusesWorldReadableWindowsDACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	raw, err := json.Marshal(credentialFile{Server: "https://one.example", AccessToken: "access-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	setWorldReadableWindowsDACL(t, path)
	t.Setenv("REMOUNT_CREDENTIAL_FILE", path)
	if got := storedAccessToken("https://one.example"); got != "" {
		t.Fatalf("storedAccessToken accepted a world-readable credential: %q", got)
	}
}

func setWorldReadableWindowsDACL(t *testing.T, path string) {
	t.Helper()
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
}
