// Package ix provisions whole Remount nodes as ix.dev machines.
package ix

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"

	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/provision/internal/providerutil"
)

// Config describes a shell-free ix.dev adapter helper. The helper wraps the
// `ix` CLI or an ix SDK, but request JSON and enrollment tokens are accepted
// only on stdin.
type Config struct {
	Helper    string
	Runner    providerutil.Runner
	APIKey    string
	Region    string
	Tenant    string
	Pool      string
	MaxOutput int
}

// Driver provisions ix.dev machines through a bounded helper protocol.
type Driver struct {
	helper providerutil.Helper
	region string
	tenant string
	pool   string
	mu     sync.Mutex
}

// New validates config. A helper is mandatory because ix.dev publishes SDKs
// and a CLI but no stable REST contract, and its documented machine surface
// has no list call — pool reconciliation needs one, so the helper owns it
// rather than this driver guessing a private HTTP shape.
func New(config Config) (*Driver, error) {
	if config.Helper == "" || config.APIKey == "" || config.Tenant == "" {
		return nil, fmt.Errorf("%w: ix helper, API key, and tenant are required", provision.ErrUnavailable)
	}
	if strings.ContainsAny(config.Helper, "\x00\r\n") {
		return nil, errors.New("ix: helper path is invalid")
	}
	if config.Runner == nil {
		if _, err := exec.LookPath(config.Helper); err != nil {
			return nil, fmt.Errorf("%w: ix helper executable is not installed", provision.ErrUnavailable)
		}
	}
	// IX_TOKEN and IX_REGION are the vendor's own variables, which its SDKs
	// and CLI already read; Remount does not invent names of its own for them.
	// An unset region is left unset so the vendor's default applies rather
	// than Remount pinning one.
	env := map[string]string{"IX_TOKEN": config.APIKey}
	if config.Region != "" {
		env["IX_REGION"] = config.Region
	}
	return &Driver{
		helper: providerutil.Helper{
			Runner: config.Runner, Executable: config.Helper, MaxOutput: config.MaxOutput,
			Env: env,
		},
		region: config.Region, tenant: config.Tenant, pool: config.Pool,
	}, nil
}

// Name implements provision.Driver.
func (*Driver) Name() string { return "ix" }

// Create starts one machine and bootstraps it through the helper's stdin.
func (d *Driver) Create(ctx context.Context, request provision.Request) (provision.Machine, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	request = provision.CloneRequest(request)
	if err := provision.ValidateSecretBoundary(request); err != nil {
		return provision.Machine{}, err
	}
	if err := request.Validate(); err != nil {
		return provision.Machine{}, err
	}
	// ix.dev places a machine by region; an omitted region falls back to the
	// helper's IX_REGION and then to the provider default. Size is not part of
	// its documented machine surface, so asking for one is unavailable rather
	// than silently ignored.
	if request.Size != "" {
		return provision.Machine{}, fmt.Errorf("%w: ix.dev does not expose stable size placement", provision.ErrUnavailable)
	}
	if request.Tenant != d.tenant || request.Labels[provision.PoolLabel] != d.pool {
		return provision.Machine{}, errors.New("ix: request is outside this driver's tenant or pool")
	}
	if existing, ok, err := d.find(ctx, request.Name); err != nil {
		return provision.Machine{}, err
	} else if ok {
		if existing.Tenant != request.Tenant {
			return provision.Machine{}, errors.New("ix: machine name belongs to another tenant")
		}
		return existing, nil
	}
	var response provision.Machine
	err := d.helper.Call(ctx, "create", helperRequest{Region: d.requestRegion(request), Request: request}, &response)
	if err != nil {
		if found, ok, findErr := d.find(ctx, request.Name); findErr == nil && ok {
			return found, nil
		}
		return provision.Machine{}, err
	}
	if response.ID == "" {
		return provision.Machine{}, errors.New("ix: create response has no machine id")
	}
	response.Provider, response.Name, response.Tenant = "ix", request.Name, request.Tenant
	response.Labels = request.Labels
	return provision.CloneMachine(response), nil
}

// Destroy permanently destroys the machine. Missing machines are success
// under the helper protocol.
func (d *Driver) Destroy(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !safeID(id) {
		return errors.New("ix: machine id is invalid")
	}
	return d.helper.Call(ctx, "destroy", helperRequest{Region: d.region, ID: id}, nil)
}

// List returns only inventory registered to this driver's tenant and pool.
func (d *Driver) List(ctx context.Context, options provision.ListOptions) ([]provision.Machine, error) {
	if options.Tenant != "" && options.Tenant != d.tenant || options.Pool != "" && options.Pool != d.pool {
		return nil, nil
	}
	var response struct {
		Machines []provision.Machine `json:"machines"`
	}
	if err := d.helper.Call(ctx, "list", helperRequest{Region: d.region, List: options}, &response); err != nil {
		return nil, err
	}
	out := make([]provision.Machine, 0, len(response.Machines))
	for _, machine := range response.Machines {
		machine.Provider = "ix"
		if machine.Tenant != d.tenant || machine.Labels[provision.PoolLabel] != d.pool {
			continue
		}
		if options.Tenant != "" && machine.Tenant != options.Tenant || options.Pool != "" && machine.Labels[provision.PoolLabel] != options.Pool {
			continue
		}
		out = append(out, provision.CloneMachine(machine))
	}
	provision.SortMachines(out)
	return out, nil
}

func (d *Driver) find(ctx context.Context, name string) (provision.Machine, bool, error) {
	machines, err := d.List(ctx, provision.ListOptions{})
	if err != nil {
		return provision.Machine{}, false, err
	}
	for _, machine := range machines {
		if machine.Name == name {
			return machine, true, nil
		}
	}
	return provision.Machine{}, false, nil
}

// requestRegion prefers the region the caller asked for and falls back to the
// driver's configured region, so a pool with one region need not repeat it on
// every request.
func (d *Driver) requestRegion(request provision.Request) string {
	if request.Region != "" {
		return request.Region
	}
	return d.region
}

type helperRequest struct {
	Region  string                `json:"region,omitempty"`
	Request provision.Request     `json:"request,omitempty"`
	List    provision.ListOptions `json:"list,omitempty"`
	ID      string                `json:"id,omitempty"`
}

func safeID(value string) bool {
	return value != "" && !strings.ContainsAny(value, "/\\\x00\r\n")
}

var _ provision.Driver = (*Driver)(nil)
