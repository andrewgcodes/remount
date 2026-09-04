package ix

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/provision/internal/providerutil"
)

type fakeRunner struct {
	commands []providerutil.Command
	current  *provision.Machine
	creates  int
}

func (r *fakeRunner) Run(_ context.Context, command providerutil.Command) ([]byte, error) {
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
		machine := provision.Machine{ID: "ix-id", Name: input.Request.Name, Tenant: input.Request.Tenant, Labels: input.Request.Labels, State: "running"}
		r.current = &machine
		return json.Marshal(machine)
	case "destroy":
		r.current = nil
		return nil, nil
	default:
		panic(operation)
	}
}

func TestHelperDriverContractAndDedicatedTenant(t *testing.T) {
	const token = "one-time-secret-canary"
	runner := &fakeRunner{}
	driver, err := New(Config{Helper: "/opt/remount-ix-helper", Runner: runner, APIKey: "api-key", Region: "us-east-1", Tenant: "tenant-a", Pool: "pool-a"})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest(token)
	machine, err := driver.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if machine.ID != "ix-id" || machine.Provider != "ix" || machine.Tenant != request.Tenant {
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
		// IX_TOKEN is the vendor's own variable. The credential must reach the
		// helper through the environment and never through argv.
		if command.Env["IX_TOKEN"] != "api-key" {
			t.Fatalf("credential not isolated to helper env under IX_TOKEN: %v", command.Env)
		}
		if command.Env["IX_REGION"] != "us-east-1" {
			t.Fatalf("region not passed to helper: %v", command.Env)
		}
	}
	foreign := request
	foreign.Tenant = "tenant-b"
	if _, err := driver.Create(context.Background(), foreign); err == nil {
		t.Fatal("accepted another tenant on dedicated account")
	}
	// ix.dev places by region, so a region is honoured rather than refused.
	regional := testRequest(token)
	regional.Name = "node-regional"
	regional.Region = "us-west-1"
	if _, err := driver.Create(context.Background(), regional); err != nil {
		t.Fatalf("regional create: %v", err)
	}
	var sawRegion bool
	for _, command := range runner.commands {
		var input helperRequest
		if json.Unmarshal(command.Stdin, &input) == nil && input.Region == "us-west-1" {
			sawRegion = true
		}
	}
	if !sawRegion {
		t.Fatal("the requested region never reached the helper")
	}
	// Size is not part of the documented machine surface.
	sized := testRequest(token)
	sized.Name = "node-sized"
	sized.Size = "big"
	if _, err := driver.Create(context.Background(), sized); !errors.Is(err, provision.ErrUnavailable) {
		t.Fatalf("size error=%v", err)
	}
	if err := driver.Destroy(context.Background(), machine.ID); err != nil {
		t.Fatal(err)
	}
}

func testRequest(token string) provision.Request {
	return provision.Request{Name: "node-a", Tenant: "tenant-a", Labels: map[string]string{provision.PoolLabel: "pool-a"}, Bootstrap: provision.Bootstrap{
		ServerURL: "https://control.example", EnrollmentToken: token, BinaryURL: "https://control.example/remount", Backend: "process", DataDir: "/var/lib/remount",
	}}
}
