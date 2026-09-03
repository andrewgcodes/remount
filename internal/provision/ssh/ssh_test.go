package ssh

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/provision/internal/providerutil"
)

type fakeRunner struct {
	commands []providerutil.Command
	present  bool
}

func (r *fakeRunner) Run(_ context.Context, command providerutil.Command) ([]byte, error) {
	r.commands = append(r.commands, command)
	operation := command.Args[len(command.Args)-1]
	switch operation {
	case "create":
		r.present = true
		return json.Marshal(provision.Machine{State: "running"})
	case "list":
		return json.Marshal(map[string]any{"present": r.present, "machine": provision.Machine{Name: "node-a", State: "running", Labels: map[string]string{provision.PoolLabel: "pool-a"}}})
	case "destroy":
		r.present = false
		return nil, nil
	default:
		panic(operation)
	}
}

func TestPinnedSSHHelperContract(t *testing.T) {
	const token = "one-time-secret-canary"
	runner := &fakeRunner{}
	driver, err := New(Config{SSH: "ssh", Runner: runner, Host: "2001:db8::1", User: "remount", IdentityFile: "/keys/id", KnownHostsFile: "/keys/known_hosts", RemoteHelper: "/usr/local/bin/remount-bootstrap", Tenant: "tenant-a", Pool: "pool-a"})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest(token)
	machine, err := driver.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if machine.ID != "2001:db8::1" || machine.Provider != "ssh" {
		t.Fatalf("machine=%+v", machine)
	}
	command := runner.commands[1]
	args := strings.Join(command.Args, " ")
	for _, required := range []string{"BatchMode=yes", "StrictHostKeyChecking=yes", "UserKnownHostsFile=/keys/known_hosts", "IdentityAgent=none", "PasswordAuthentication=no", "remount@[2001:db8::1]", "/usr/local/bin/remount-bootstrap create"} {
		if !strings.Contains(args, required) {
			t.Fatalf("missing %q in %s", required, args)
		}
	}
	if strings.Contains(args, token) || !strings.Contains(string(command.Stdin), token) {
		t.Fatalf("secret transport args=%s stdin=%s", args, command.Stdin)
	}
	if _, err := driver.Create(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	creates := 0
	for _, candidate := range runner.commands {
		if candidate.Args[len(candidate.Args)-1] == "create" {
			creates++
		}
	}
	if creates != 1 {
		t.Fatalf("idempotent create invoked helper %d times", creates)
	}
	listed, err := driver.List(context.Background(), provision.ListOptions{Tenant: "tenant-a", Pool: "pool-a"})
	if err != nil || len(listed) != 1 {
		t.Fatalf("list=%+v err=%v", listed, err)
	}
	if err := driver.Destroy(context.Background(), machine.ID); err != nil {
		t.Fatal(err)
	}
}

func TestSSHRejectsRemoteShellSyntaxAndForeignTenant(t *testing.T) {
	_, err := New(Config{Host: "host.example", User: "user", IdentityFile: "/id", KnownHostsFile: "/known", RemoteHelper: "/bin/helper;evil", Tenant: "tenant-a"})
	if err == nil {
		t.Fatal("accepted remote shell syntax")
	}
	driver, err := New(Config{Runner: &fakeRunner{}, Host: "host.example", User: "user", IdentityFile: "/id", KnownHostsFile: "/known", RemoteHelper: "/bin/helper", Tenant: "tenant-a", Pool: "pool-a"})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest("secret")
	request.Tenant = "tenant-b"
	if _, err := driver.Create(context.Background(), request); err == nil {
		t.Fatal("accepted foreign tenant")
	}
}

func testRequest(token string) provision.Request {
	return provision.Request{Name: "node-a", Tenant: "tenant-a", Labels: map[string]string{provision.PoolLabel: "pool-a"}, Bootstrap: provision.Bootstrap{
		ServerURL: "https://control.example", EnrollmentToken: token, BinaryURL: "https://control.example/remount", Backend: "process", DataDir: "/var/lib/remount",
	}}
}
