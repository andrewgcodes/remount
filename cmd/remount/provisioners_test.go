package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadProvisionersUsesCredentialEnvironmentReferences(t *testing.T) {
	t.Setenv("TEST_E2B_TOKEN", "secret-value-that-must-not-be-in-config")
	path := filepath.Join(t.TempDir(), "provisioners.json")
	raw := `{
  "bootstrap":{"server_url":"https://control.example","binary_url":"https://control.example/remount","data_dir":"/var/lib/remount"},
  "drivers":[{"vendor":"e2b","token_env":"TEST_E2B_TOKEN","template":"remount-node"}]
}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	drivers, bootstrap, err := loadProvisioners(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(drivers) != 1 || drivers[0].Name() != "e2b" || bootstrap.ServerURL != "https://control.example" {
		t.Fatalf("provisioners = %v bootstrap=%+v", drivers, bootstrap)
	}
	config, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(config), "secret-value") {
		t.Fatal("credential value entered provisioner configuration")
	}
}

func TestLoadProvisionersFailsClosedForMissingCredential(t *testing.T) {
	path := filepath.Join(t.TempDir(), "provisioners.json")
	if err := os.WriteFile(path, []byte(`{"bootstrap":{},"drivers":[{"vendor":"e2b","token_env":"MISSING_TEST_TOKEN","template":"node"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, err := loadProvisioners(path)
	if err == nil || !strings.Contains(err.Error(), "MISSING_TEST_TOKEN") {
		t.Fatalf("missing credential error = %v", err)
	}
}
