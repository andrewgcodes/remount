//go:build integration

package ssh

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"remount.dev/remount/internal/provision"
)

func TestLiveProvisionerLifecycle(t *testing.T) {
	values := requireLiveEnv(t, "REMOUNT_SSH_HOST", "REMOUNT_SSH_USER", "REMOUNT_SSH_IDENTITY_FILE", "REMOUNT_SSH_KNOWN_HOSTS_FILE", "REMOUNT_VENDOR_SERVER_URL", "REMOUNT_VENDOR_ENROLL_TOKEN", "REMOUNT_VENDOR_BINARY_URL", "REMOUNT_VENDOR_TENANT", "REMOUNT_VENDOR_POOL")
	port := 22
	if raw := os.Getenv("REMOUNT_SSH_PORT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("REMOUNT_SSH_PORT is invalid")
		}
		port = parsed
	}
	driver, err := New(Config{SSH: os.Getenv("REMOUNT_SSH_BIN"), Host: values["REMOUNT_SSH_HOST"], Port: port, User: values["REMOUNT_SSH_USER"], IdentityFile: values["REMOUNT_SSH_IDENTITY_FILE"], KnownHostsFile: values["REMOUNT_SSH_KNOWN_HOSTS_FILE"], RemoteHelper: os.Getenv("REMOUNT_SSH_REMOTE_HELPER"), Tenant: values["REMOUNT_VENDOR_TENANT"], Pool: values["REMOUNT_VENDOR_POOL"]})
	if err != nil {
		t.Fatal(err)
	}
	runLiveLifecycle(t, driver, liveRequest(values))
}

func liveRequest(values map[string]string) provision.Request {
	backend := os.Getenv("REMOUNT_VENDOR_BACKEND")
	if backend == "" {
		backend = "process"
	}
	dataDir := os.Getenv("REMOUNT_VENDOR_DATA_DIR")
	if dataDir == "" {
		dataDir = "/var/lib/remount"
	}
	return provision.Request{Name: fmt.Sprintf("remount-ci-%d", time.Now().UnixNano()), Tenant: values["REMOUNT_VENDOR_TENANT"], Labels: map[string]string{provision.PoolLabel: values["REMOUNT_VENDOR_POOL"]}, Bootstrap: provision.Bootstrap{ServerURL: values["REMOUNT_VENDOR_SERVER_URL"], EnrollmentToken: values["REMOUNT_VENDOR_ENROLL_TOKEN"], BinaryURL: values["REMOUNT_VENDOR_BINARY_URL"], Backend: backend, DataDir: dataDir}}
}

func runLiveLifecycle(t *testing.T, driver provision.Driver, request provision.Request) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	machine, err := driver.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	id := machine.ID
	defer func() {
		if id != "" {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
			defer stop()
			if err := driver.Destroy(cleanup, id); err != nil {
				t.Errorf("cleanup host install: %v", err)
			}
		}
	}()
	machines, err := driver.List(ctx, provision.ListOptions{Tenant: request.Tenant, Pool: request.Labels[provision.PoolLabel]})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, candidate := range machines {
		if candidate.ID == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("created host install %q absent from inventory", id)
	}
	if err := driver.Destroy(ctx, id); err != nil {
		t.Fatal(err)
	}
	id = ""
}

func requireLiveEnv(t *testing.T, names ...string) map[string]string {
	t.Helper()
	values := make(map[string]string, len(names))
	var missing []string
	for _, name := range names {
		values[name] = os.Getenv(name)
		if values[name] == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		t.Skipf("SKIPPED unavailable: missing required environment names %v", missing)
	}
	return values
}
