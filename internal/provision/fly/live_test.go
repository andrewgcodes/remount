//go:build integration

package fly

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/transport"
)

func TestLiveProvisionerLifecycle(t *testing.T) {
	values := requireLiveEnv(t, "FLY_IO_2026_CORRECT_KEY", "REMOUNT_FLY_APP", "REMOUNT_FLY_IMAGE", "REMOUNT_VENDOR_SERVER_URL", "REMOUNT_VENDOR_ENROLL_TOKEN", "REMOUNT_VENDOR_BINARY_URL", "REMOUNT_VENDOR_TENANT", "REMOUNT_VENDOR_POOL")
	driver, err := New(Config{Token: values["FLY_IO_2026_CORRECT_KEY"], App: values["REMOUNT_FLY_APP"], Image: values["REMOUNT_FLY_IMAGE"]})
	if err != nil {
		t.Fatal(err)
	}
	runLiveLifecycle(t, driver, liveRequest(values, os.Getenv("REMOUNT_FLY_REGION"), os.Getenv("REMOUNT_FLY_SIZE")))
}

func TestLiveProvisionerEnrollmentReadiness(t *testing.T) {
	values := requireLiveEnv(t, "FLY_IO_2026_CORRECT_KEY", "REMOUNT_FLY_APP", "REMOUNT_FLY_IMAGE", "REMOUNT_VENDOR_SERVER_URL", "REMOUNT_VENDOR_CONTROL_TOKEN", "REMOUNT_VENDOR_ENROLL_TOKEN", "REMOUNT_VENDOR_BINARY_URL", "REMOUNT_VENDOR_TENANT", "REMOUNT_VENDOR_POOL", "REMOUNT_VENDOR_EXPECT_VERSION")
	driver, err := New(Config{Token: values["FLY_IO_2026_CORRECT_KEY"], App: values["REMOUNT_FLY_APP"], Image: values["REMOUNT_FLY_IMAGE"]})
	if err != nil {
		t.Fatal(err)
	}
	remount := client.New(client.Options{
		Dialer: liveDialer(values["REMOUNT_VENDOR_SERVER_URL"]),
		Token:  values["REMOUNT_VENDOR_CONTROL_TOKEN"],
	})
	defer remount.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	before, err := remount.ListNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	existing := make(map[string]bool, len(before))
	for _, node := range before {
		existing[node.ID] = true
	}
	request := liveRequest(values, os.Getenv("REMOUNT_FLY_REGION"), os.Getenv("REMOUNT_FLY_SIZE"))
	machine, err := driver.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		if err := driver.Destroy(cleanup, machine.ID); err != nil {
			t.Errorf("cleanup machine: %v", err)
		}
	}()
	var nodeID string
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for nodeID == "" {
		nodes, err := remount.ListNodes(ctx)
		if err == nil {
			for _, node := range nodes {
				if !existing[node.ID] && node.Online && node.Labels["vendor"] == "fly" && node.Info.Version == values["REMOUNT_VENDOR_EXPECT_VERSION"] {
					nodeID = node.ID
					break
				}
			}
		}
		if nodeID != "" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-deadline.C:
			t.Fatal("Fly Machine started but no exact-candidate Remount node enrolled")
		case <-ticker.C:
		}
	}
	if _, err := remount.NodeDiag(ctx, nodeID, "", false); err != nil {
		t.Fatalf("enrolled Fly node diagnostic: %v", err)
	}
}

func liveDialer(server string) transport.Dialer {
	link := strings.TrimSuffix(server, "/")
	link = strings.Replace(link, "http://", "ws://", 1)
	link = strings.Replace(link, "https://", "wss://", 1)
	return transport.DialFunc(func(ctx context.Context) (transport.Conn, error) {
		return transport.DialWS(ctx, link+"/v1/link", nil)
	})
}

func liveRequest(values map[string]string, region, size string) provision.Request {
	backend := os.Getenv("REMOUNT_VENDOR_BACKEND")
	if backend == "" {
		backend = "process"
	}
	dataDir := os.Getenv("REMOUNT_VENDOR_DATA_DIR")
	if dataDir == "" {
		dataDir = "/var/lib/remount"
	}
	return provision.Request{Name: fmt.Sprintf("remount-ci-%d", time.Now().UnixNano()), Tenant: values["REMOUNT_VENDOR_TENANT"], Region: region, Size: size, Labels: map[string]string{provision.PoolLabel: values["REMOUNT_VENDOR_POOL"]}, Bootstrap: provision.Bootstrap{ServerURL: values["REMOUNT_VENDOR_SERVER_URL"], EnrollmentToken: values["REMOUNT_VENDOR_ENROLL_TOKEN"], BinaryURL: values["REMOUNT_VENDOR_BINARY_URL"], Backend: backend, DataDir: dataDir}}
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
				t.Errorf("cleanup machine: %v", err)
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
		t.Fatalf("created machine %q absent from inventory", id)
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
