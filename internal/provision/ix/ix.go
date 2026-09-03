// Package ix provisions whole Remount nodes in iximiuz Labs playgrounds.
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

// Config describes a shell-free ix adapter helper. The helper may wrap
// `labctl playground` and `labctl ssh`, but request JSON and enrollment tokens
// are accepted only on stdin.
type Config struct {
	Helper     string
	Runner     providerutil.Runner
	Playground string
	Account    string
	APIKey     string
	Tenant     string
	Pool       string
	MaxOutput  int
}

// Driver provisions iximiuz playground VMs through a bounded helper protocol.
type Driver struct {
	helper     providerutil.Helper
	playground string
	account    string
	tenant     string
	pool       string
	mu         sync.Mutex
}

// New validates config. A helper is mandatory because labctl has no stable
// machine JSON API or metadata contract sufficient for pool reconciliation.
func New(config Config) (*Driver, error) {
	if config.Helper == "" || config.Playground == "" || config.Account == "" || config.APIKey == "" || config.Tenant == "" {
		return nil, fmt.Errorf("%w: ix helper, playground, API key, dedicated account, and tenant are required", provision.ErrUnavailable)
	}
	if strings.ContainsAny(config.Helper, "\x00\r\n") {
		return nil, errors.New("ix: helper path is invalid")
	}
	if config.Runner == nil {
		if _, err := exec.LookPath(config.Helper); err != nil {
			return nil, fmt.Errorf("%w: ix helper executable is not installed", provision.ErrUnavailable)
		}
	}
	return &Driver{
		helper: providerutil.Helper{
			Runner: config.Runner, Executable: config.Helper, MaxOutput: config.MaxOutput,
			Env: map[string]string{"IX_DEV_API_KEY": config.APIKey},
		},
		playground: config.Playground, account: config.Account, tenant: config.Tenant, pool: config.Pool,
	}, nil
}

// Name implements provision.Driver.
func (*Driver) Name() string { return "ix" }

// Create starts one playground VM and bootstraps it through the helper's stdin.
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
	if request.Region != "" || request.Size != "" {
		return provision.Machine{}, fmt.Errorf("%w: ix playgrounds do not expose stable region or size placement", provision.ErrUnavailable)
	}
	if request.Tenant != d.tenant || request.Labels[provision.PoolLabel] != d.pool {
		return provision.Machine{}, errors.New("ix: request is outside the dedicated account's tenant or pool")
	}
	if existing, ok, err := d.find(ctx, request.Name); err != nil {
		return provision.Machine{}, err
	} else if ok {
		if existing.Tenant != request.Tenant {
			return provision.Machine{}, errors.New("ix: playground name belongs to another tenant")
		}
		return existing, nil
	}
	var response provision.Machine
	err := d.helper.Call(ctx, "create", helperRequest{Playground: d.playground, Account: d.account, Request: request}, &response)
	if err != nil {
		if found, ok, findErr := d.find(ctx, request.Name); findErr == nil && ok {
			return found, nil
		}
		return provision.Machine{}, err
	}
	if response.ID == "" {
		return provision.Machine{}, errors.New("ix: create response has no playground id")
	}
	response.Provider, response.Name, response.Tenant = "ix", request.Name, request.Tenant
	response.Labels = request.Labels
	return provision.CloneMachine(response), nil
}

// Destroy permanently destroys the playground instance. Missing instances are
// success under the helper protocol.
func (d *Driver) Destroy(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !safeID(id) {
		return errors.New("ix: playground id is invalid")
	}
	return d.helper.Call(ctx, "destroy", helperRequest{Playground: d.playground, Account: d.account, ID: id}, nil)
}

// List returns only inventory registered to this dedicated account and the
// requested tenant/pool.
func (d *Driver) List(ctx context.Context, options provision.ListOptions) ([]provision.Machine, error) {
	if options.Tenant != "" && options.Tenant != d.tenant || options.Pool != "" && options.Pool != d.pool {
		return nil, nil
	}
	var response struct {
		Machines []provision.Machine `json:"machines"`
	}
	if err := d.helper.Call(ctx, "list", helperRequest{Playground: d.playground, Account: d.account, List: options}, &response); err != nil {
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

type helperRequest struct {
	Playground string                `json:"playground"`
	Account    string                `json:"account"`
	Request    provision.Request     `json:"request,omitempty"`
	List       provision.ListOptions `json:"list,omitempty"`
	ID         string                `json:"id,omitempty"`
}

func safeID(value string) bool {
	return value != "" && !strings.ContainsAny(value, "/\\\x00\r\n")
}

var _ provision.Driver = (*Driver)(nil)
