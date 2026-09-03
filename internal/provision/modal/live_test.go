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
	listOptions := provision.ListOptions{Tenant: request.Tenant, Pool: request.Labels[provision.PoolLabel]}
	var id string
	recoverByName := false
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		if id == "" && recoverByName {
			recovery, recoveryStop := context.WithTimeout(cleanup, 30*time.Second)
			recoveredID, err := recoverMachineIDByName(recovery, driver, listOptions, request.Name)
			recoveryStop()
			if err != nil {
				t.Errorf("recover ambiguous sandbox create: %v", err)
			} else {
				id = recoveredID
			}
		}
		if id != "" {
			if err := destroyAndWaitAbsent(cleanup, driver, listOptions, id); err != nil {
				t.Errorf("cleanup sandbox: %v", err)
			}
		}
	}()
	machine, err := driver.Create(ctx, request)
	id = machine.ID
	if err != nil {
		recoverByName = id == ""
		t.Fatal(err)
	}
	if id == "" {
		recoverByName = true
		t.Fatal("created sandbox has no provider id")
	}
	machines, err := driver.List(ctx, listOptions)
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
	if err := destroyAndWaitAbsent(ctx, driver, listOptions, id); err != nil {
		t.Fatal(err)
	}
	id = ""
}

func destroyAndWaitAbsent(ctx context.Context, driver provision.Driver, options provision.ListOptions, id string) error {
	if err := driver.Destroy(ctx, id); err != nil {
		return err
	}
	var lastErr error
	for {
		machines, err := driver.List(ctx, options)
		if err == nil {
			present := false
			for _, machine := range machines {
				if machine.ID == id {
					present = true
					break
				}
			}
			if !present {
				return nil
			}
			lastErr = fmt.Errorf("sandbox %q remains in provider inventory", id)
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("verify sandbox deletion: %w (last inventory result: %v)", ctx.Err(), lastErr)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func recoverMachineIDByName(ctx context.Context, driver provision.Driver, options provision.ListOptions, name string) (string, error) {
	var lastErr error
	sawInventory := false
	for {
		machines, err := driver.List(ctx, options)
		if err == nil {
			sawInventory = true
			lastErr = nil
			for _, machine := range machines {
				if machine.Name == name {
					if machine.ID == "" {
						return "", fmt.Errorf("recovered sandbox %q has no provider id", name)
					}
					return machine.ID, nil
				}
			}
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return "", fmt.Errorf("inventory unavailable while recovering %q: %w (last error: %v)", name, ctx.Err(), lastErr)
			}
			if sawInventory {
				return "", nil
			}
			return "", fmt.Errorf("inventory unavailable while recovering %q: %w", name, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

type liveCleanupProbe struct {
	machine            provision.Machine
	destroyed          bool
	destroyCalls       int
	listCalls          int
	remainAfterDestroy int
	revealAfter        int
}

func (*liveCleanupProbe) Name() string { return "cleanup-probe" }

func (p *liveCleanupProbe) Create(context.Context, provision.Request) (provision.Machine, error) {
	return p.machine, nil
}

func (p *liveCleanupProbe) Destroy(context.Context, string) error {
	p.destroyed = true
	p.destroyCalls++
	return nil
}

func (p *liveCleanupProbe) List(context.Context, provision.ListOptions) ([]provision.Machine, error) {
	p.listCalls++
	if p.destroyed {
		if p.listCalls <= p.remainAfterDestroy {
			return []provision.Machine{p.machine}, nil
		}
		return nil, nil
	}
	if p.listCalls <= p.revealAfter {
		return nil, nil
	}
	return []provision.Machine{p.machine}, nil
}

func TestLiveCleanupHelpers(t *testing.T) {
	machine := provision.Machine{ID: "provider-id", Name: "unique-name", Tenant: "tenant", Labels: map[string]string{provision.PoolLabel: "pool"}}
	options := provision.ListOptions{Tenant: "tenant", Pool: "pool"}

	destroyProbe := &liveCleanupProbe{machine: machine, remainAfterDestroy: 2}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := destroyAndWaitAbsent(ctx, destroyProbe, options, machine.ID); err != nil {
		t.Fatal(err)
	}
	if destroyProbe.destroyCalls != 1 || destroyProbe.listCalls != 3 {
		t.Fatalf("cleanup calls: destroy=%d list=%d", destroyProbe.destroyCalls, destroyProbe.listCalls)
	}

	recoveryProbe := &liveCleanupProbe{machine: machine, revealAfter: 2}
	recovered, err := recoverMachineIDByName(ctx, recoveryProbe, options, machine.Name)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != machine.ID || recoveryProbe.listCalls != 3 {
		t.Fatalf("recovered=%q list=%d", recovered, recoveryProbe.listCalls)
	}
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
