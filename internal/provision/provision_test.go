package provision

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func validRequest() Request {
	return Request{
		Name: "pool-a-1", Tenant: "tenant-a", Region: "iad", Size: "shared-cpu-1x",
		Labels: map[string]string{"pool": "a", "region": "iad"},
		Bootstrap: Bootstrap{
			ServerURL: "https://remount.example", EnrollmentToken: "enroll-secret",
			BinaryURL: "https://remount.example/releases/remount-linux-amd64",
			Backend:   "gvisor", DataDir: "/var/lib/remount",
		},
	}
}

func TestRequestValidateAndNodeArgsKeepEnrollmentOutOfArgv(t *testing.T) {
	req := validRequest()
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
	args := req.Bootstrap.NodeArgs()
	if strings.Contains(strings.Join(args, "\x00"), req.Bootstrap.EnrollmentToken) {
		t.Fatal("node argv contains the enrollment token")
	}
	want := []string{"up", "--server", "https://remount.example", "--backend", "gvisor", "--data", "/var/lib/remount"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("args = %#v, want %#v", args, want)
	}
}

func TestRequestValidateRejectsUnsafeOrIncompleteBootstrap(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Request)
		want string
	}{
		{"name", func(r *Request) { r.Name = "" }, "machine name"},
		{"tenant", func(r *Request) { r.Tenant = "" }, "tenant"},
		{"token", func(r *Request) { r.Bootstrap.EnrollmentToken = "" }, "enrollment token"},
		{"server", func(r *Request) { r.Bootstrap.ServerURL = "file:///tmp/socket" }, "server URL"},
		{"binary", func(r *Request) { r.Bootstrap.BinaryURL = "relative" }, "binary URL"},
		{"backend", func(r *Request) { r.Bootstrap.Backend = "" }, "backend"},
		{"data", func(r *Request) { r.Bootstrap.DataDir = "relative" }, "data directory"},
		{"label", func(r *Request) { r.Labels["bad\nkey"] = "x" }, "control character"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validRequest()
			tt.edit(&req)
			if err := req.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestCloneAndSortDoNotExposeMutableProviderState(t *testing.T) {
	req := validRequest()
	copyReq := CloneRequest(req)
	copyReq.Labels["pool"] = "changed"
	if req.Labels["pool"] != "a" {
		t.Fatal("CloneRequest returned the caller's labels map")
	}

	now := time.Unix(100, 0)
	machines := []Machine{
		{ID: "m_b", CreatedAt: now},
		{ID: "m_c", CreatedAt: now.Add(time.Second)},
		{ID: "m_a", CreatedAt: now, Labels: map[string]string{"pool": "a"}},
	}
	cloned := CloneMachine(machines[2])
	cloned.Labels["pool"] = "changed"
	if machines[2].Labels["pool"] != "a" {
		t.Fatal("CloneMachine returned the provider's labels map")
	}
	SortMachines(machines)
	got := []string{machines[0].ID, machines[1].ID, machines[2].ID}
	want := []string{"m_a", "m_b", "m_c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}
