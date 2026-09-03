//go:build integration

package modal

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"remount.dev/remount/internal/provision"
)

func TestLiveProvisionerLifecycle(t *testing.T) {
	values := requireLiveEnv(t, "MODAL_TOKEN_ID", "MODAL_TOKEN_SECRET", "REMOUNT_MODAL_HELPER", "REMOUNT_MODAL_APP", "REMOUNT_MODAL_IMAGE", "REMOUNT_VENDOR_SERVER_URL", "REMOUNT_VENDOR_ENROLL_TOKEN", "REMOUNT_VENDOR_BINARY_URL", "REMOUNT_VENDOR_TENANT", "REMOUNT_VENDOR_POOL")
	driver, err := New(Config{Helper: values["REMOUNT_MODAL_HELPER"], Environment: os.Getenv("MODAL_ENVIRONMENT"), App: values["REMOUNT_MODAL_APP"], Image: values["REMOUNT_MODAL_IMAGE"], TokenID: values["MODAL_TOKEN_ID"], TokenSecret: values["MODAL_TOKEN_SECRET"]})
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
	return provision.Request{Name: fmt.Sprintf("remount-ci-%d", time.Now().UnixNano()), Tenant: values["REMOUNT_VENDOR_TENANT"], Region: os.Getenv("REMOUNT_MODAL_REGION"), Size: os.Getenv("REMOUNT_MODAL_SIZE"), Labels: map[string]string{provision.PoolLabel: values["REMOUNT_VENDOR_POOL"]}, Bootstrap: provision.Bootstrap{ServerURL: values["REMOUNT_VENDOR_SERVER_URL"], EnrollmentToken: values["REMOUNT_VENDOR_ENROLL_TOKEN"], BinaryURL: values["REMOUNT_VENDOR_BINARY_URL"], Backend: backend, DataDir: dataDir}}
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
				t.Errorf("cleanup sandbox: %v", err)
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
		t.Fatalf("created sandbox %q absent from inventory", id)
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
