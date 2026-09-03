// Package provision defines the control-plane contract for obtaining whole
// Remount nodes from infrastructure providers. Providers create machines;
// workspace backends running on those machines still own isolation and report
// their own capabilities.
package provision

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

// ErrNotFound means the provider no longer has the requested machine.
var ErrNotFound = errors.New("provision: machine not found")

// Driver creates and destroys whole machines that run one Remount node.
// Implementations must not retain or log Bootstrap.EnrollmentToken after
// Create returns.
type Driver interface {
	Name() string
	Create(context.Context, Request) (Machine, error)
	Destroy(context.Context, string) error
	List(context.Context, ListOptions) ([]Machine, error)
}

// Request is one provider machine request. Tenant is an isolation boundary:
// a vendor-pool machine is assigned to exactly one tenant for its lifetime.
type Request struct {
	Name      string
	Tenant    string
	Region    string
	Size      string
	Labels    map[string]string
	Bootstrap Bootstrap
}

// Bootstrap is the secret-bearing input needed to start a Remount node.
// EnrollmentToken is intentionally absent from Machine and provider-visible
// labels. Drivers pass it through their platform's protected environment, not
// in argv, metadata tags, logs, or a reusable image.
type Bootstrap struct {
	ServerURL       string
	EnrollmentToken string
	BinaryURL       string
	Backend         string
	DataDir         string
}

// Machine is the non-secret provider state retained by a pool reconciler.
type Machine struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Provider  string            `json:"provider"`
	Tenant    string            `json:"tenant"`
	Region    string            `json:"region,omitempty"`
	State     string            `json:"state,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	CreatedAt time.Time         `json:"created_at,omitempty"`
}

// ListOptions scopes provider inventory to one pool and tenant. Drivers must
// never return machines outside both filters when either is non-empty.
type ListOptions struct {
	Pool   string
	Tenant string
}

// Validate checks the provider-independent admission boundary before a
// driver makes an external request.
func (r Request) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return errors.New("provision: machine name is required")
	}
	if strings.TrimSpace(r.Tenant) == "" {
		return errors.New("provision: tenant is required")
	}
	if err := validateLabelSet(r.Labels); err != nil {
		return err
	}
	return r.Bootstrap.Validate()
}

// Validate checks that a provider can start a node without falling back to a
// baked-in credential or ambiguous endpoint.
func (b Bootstrap) Validate() error {
	if strings.TrimSpace(b.EnrollmentToken) == "" {
		return errors.New("provision: one-time enrollment token is required")
	}
	server, err := url.Parse(b.ServerURL)
	if err != nil || server.Host == "" || (server.Scheme != "https" && server.Scheme != "http") {
		return fmt.Errorf("provision: server URL must be an absolute http(s) URL")
	}
	if strings.TrimSpace(b.BinaryURL) == "" {
		return errors.New("provision: candidate binary URL is required")
	}
	binary, err := url.Parse(b.BinaryURL)
	if err != nil || binary.Host == "" || (binary.Scheme != "https" && binary.Scheme != "http") {
		return fmt.Errorf("provision: binary URL must be an absolute http(s) URL")
	}
	if strings.TrimSpace(b.Backend) == "" {
		return errors.New("provision: backend is required")
	}
	if strings.TrimSpace(b.DataDir) == "" || !strings.HasPrefix(b.DataDir, "/") {
		return errors.New("provision: node data directory must be absolute")
	}
	return nil
}

// NodeArgs returns secret-free arguments for the candidate binary. The
// provider supplies REMOUNT_ENROLL_TOKEN separately in its protected
// environment; putting a one-time credential in argv makes it observable to
// other processes and provider diagnostics.
func (b Bootstrap) NodeArgs() []string {
	return []string{
		"up", "--server", b.ServerURL,
		"--backend", b.Backend,
		"--data", b.DataDir,
	}
}

// CloneRequest gives an asynchronous driver private maps while preserving the
// intentionally short-lived enrollment token for the duration of Create.
func CloneRequest(in Request) Request {
	out := in
	out.Labels = cloneMap(in.Labels)
	return out
}

// CloneMachine prevents provider-owned inventory maps from escaping into the
// reconciler and being mutated concurrently.
func CloneMachine(in Machine) Machine {
	out := in
	out.Labels = cloneMap(in.Labels)
	return out
}

// SortMachines makes inventory and reconciliation deterministic.
func SortMachines(machines []Machine) {
	sort.Slice(machines, func(i, j int) bool {
		if machines[i].CreatedAt.Equal(machines[j].CreatedAt) {
			return machines[i].ID < machines[j].ID
		}
		return machines[i].CreatedAt.Before(machines[j].CreatedAt)
	})
}

func validateLabelSet(labels map[string]string) error {
	for key, value := range labels {
		if strings.TrimSpace(key) == "" {
			return errors.New("provision: label key is empty")
		}
		if strings.ContainsAny(key, "=\x00\n\r") || strings.ContainsAny(value, "\x00\n\r") {
			return fmt.Errorf("provision: label %q contains a control character", key)
		}
	}
	return nil
}

func cloneMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
