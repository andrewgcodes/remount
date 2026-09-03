package modal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/provision/internal/providerutil"
)

type fakeRunner struct {
	mu       sync.Mutex
	commands []providerutil.Command
	current  *provision.Machine
	creates  int
}

func (r *fakeRunner) Run(_ context.Context, command providerutil.Command) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = append(r.commands, command)
	operation := command.Args[len(command.Args)-1]
	switch operation {
	case "list":
		machines := []provision.Machine{}
		if r.current != nil {
			machines = append(machines, *r.current)
		}
		return json.Marshal(map[string]any{"machines": machines})
	case "create":
		r.creates++
		var input helperRequest
		if err := json.Unmarshal(command.Stdin, &input); err != nil {
			return nil, err
		}
		machine := provision.Machine{ID: "modal-id", Name: input.Request.Name, Tenant: input.Request.Tenant, Labels: input.Request.Labels, State: "running"}
		r.current = &machine
		return json.Marshal(machine)
	case "destroy":
		r.current = nil
		return nil, nil
	default:
		panic(operation)
	}
}

func TestHelperDriverContractAndSecretTransport(t *testing.T) {
	const token = "one-time-secret-canary"
	runner := &fakeRunner{}
	driver, err := New(Config{Helper: "/opt/remount-modal-helper", Runner: runner, Environment: "dev", App: "app", Image: "image", TokenID: "token-id", TokenSecret: "token-secret", Network: &NetworkConfig{OutboundCIDRAllowlist: []string{"203.0.113.1/32"}}})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest(token)
	machine, err := driver.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if machine.ID != "modal-id" || machine.Provider != "modal" || machine.Tenant != request.Tenant {
		t.Fatalf("machine=%+v", machine)
	}
	if _, err := driver.Create(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if runner.creates != 1 {
		t.Fatalf("idempotent create count=%d", runner.creates)
	}
	for _, command := range runner.commands {
		if strings.Contains(strings.Join(command.Args, " "), token) {
			t.Fatalf("token in argv: %v", command.Args)
		}
		if command.Env["MODAL_TOKEN_ID"] != "token-id" || command.Env["MODAL_TOKEN_SECRET"] != "token-secret" {
			t.Fatalf("credentials not isolated to helper env: %v", command.Env)
		}
	}
	if !strings.Contains(string(runner.commands[1].Stdin), token) {
		t.Fatal("create token did not travel on stdin")
	}
	if !strings.Contains(string(runner.commands[1].Stdin), "outbound_cidr_allowlist") {
		t.Fatal("network defense did not reach helper")
	}
	if err := driver.Destroy(context.Background(), machine.ID); err != nil {
		t.Fatal(err)
	}
}

func TestMissingHelperIsUnavailable(t *testing.T) {
	_, err := New(Config{Helper: "/definitely/not/a/remount-helper", App: "app", Image: "image", TokenID: "id", TokenSecret: "secret"})
	if !errors.Is(err, provision.ErrUnavailable) {
		t.Fatalf("error=%v", err)
	}
}

func testRequest(token string) provision.Request {
	return provision.Request{Name: "node-a", Tenant: "tenant-a", Region: "us-east", Size: "cpu=2", Labels: map[string]string{provision.PoolLabel: "pool-a"}, Bootstrap: provision.Bootstrap{
		ServerURL: "https://control.example", EnrollmentToken: token, BinaryURL: "https://control.example/remount", Backend: "gvisor", DataDir: "/var/lib/remount",
	}}
}
