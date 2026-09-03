// Package modal provisions whole Remount nodes as Modal Sandboxes.
package modal

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

// Config describes a shell-free Modal helper. Modal does not publish a stable
// REST Sandbox API; the helper is an independently versioned adapter built on
// Modal's supported SDK and reads all request data from stdin.
type Config struct {
	Helper      string
	Runner      providerutil.Runner
	Environment string
	App         string
	Image       string
	TokenID     string
	TokenSecret string
	Network     *NetworkConfig
	MaxOutput   int
}

// NetworkConfig is optional defense-in-depth Modal networking. It never
// contributes to the backend capability descriptor.
type NetworkConfig struct {
	BlockNetwork            bool     `json:"block_network,omitempty"`
	OutboundCIDRAllowlist   []string `json:"outbound_cidr_allowlist,omitempty"`
	OutboundDomainAllowlist []string `json:"outbound_domain_allowlist,omitempty"`
	InboundCIDRAllowlist    []string `json:"inbound_cidr_allowlist,omitempty"`
}

// Driver provisions Modal Sandboxes through the bounded helper protocol.
type Driver struct {
	helper      providerutil.Helper
	environment string
	app         string
	image       string
	network     *NetworkConfig
	mu          sync.Mutex
}

// New validates the adapter configuration. Missing helper configuration is
// unavailable, never treated as a healthy Modal integration.
func New(config Config) (*Driver, error) {
	if config.Helper == "" || config.App == "" || config.Image == "" || config.TokenID == "" || config.TokenSecret == "" {
		return nil, fmt.Errorf("%w: Modal helper, app, image, and API credentials are required", provision.ErrUnavailable)
	}
	if strings.ContainsAny(config.Helper, "\x00\r\n") {
		return nil, errors.New("modal: helper path is invalid")
	}
	if config.Runner == nil {
		if _, err := exec.LookPath(config.Helper); err != nil {
			return nil, fmt.Errorf("%w: Modal helper executable is not installed", provision.ErrUnavailable)
		}
	}
	network, err := cloneNetwork(config.Network)
	if err != nil {
		return nil, err
	}
	return &Driver{
		helper: providerutil.Helper{
			Runner: config.Runner, Executable: config.Helper, MaxOutput: config.MaxOutput,
			Env: map[string]string{"MODAL_TOKEN_ID": config.TokenID, "MODAL_TOKEN_SECRET": config.TokenSecret},
		},
		environment: config.Environment, app: config.App, image: config.Image, network: network,
	}, nil
}

// Name implements provision.Driver.
func (*Driver) Name() string { return "modal" }

// Create starts a named Sandbox whose configured image bootstrap reads the
// REMOUNT_* environment supplied through the helper's protected API request.
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
	if existing, ok, err := d.find(ctx, request.Name); err != nil {
		return provision.Machine{}, err
	} else if ok {
		if existing.Tenant != request.Tenant {
			return provision.Machine{}, errors.New("modal: sandbox name belongs to another tenant")
		}
		return existing, nil
	}
	var response provision.Machine
	err := d.helper.Call(ctx, "create", helperRequest{Config: d.config(), Request: request}, &response)
	if err != nil {
		if found, ok, findErr := d.find(ctx, request.Name); findErr == nil && ok {
			return found, nil
		}
		return provision.Machine{}, err
	}
	if response.ID == "" {
		return provision.Machine{}, errors.New("modal: create response has no sandbox id")
	}
	return normalize(response, request), nil
}

// Destroy terminates a Sandbox. The helper contract treats an absent ID as a
// successful idempotent destroy.
func (d *Driver) Destroy(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !safeID(id) {
		return errors.New("modal: sandbox id is invalid")
	}
	return d.helper.Call(ctx, "destroy", helperRequest{Config: d.config(), ID: id}, nil)
}

// List returns helper inventory after enforcing tenant and pool filters again
// at the Remount boundary.
func (d *Driver) List(ctx context.Context, options provision.ListOptions) ([]provision.Machine, error) {
	var response struct {
		Machines []provision.Machine `json:"machines"`
	}
	if err := d.helper.Call(ctx, "list", helperRequest{Config: d.config(), List: options}, &response); err != nil {
		return nil, err
	}
	out := make([]provision.Machine, 0, len(response.Machines))
	for _, machine := range response.Machines {
		machine.Provider = "modal"
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

type helperConfig struct {
	Environment string         `json:"environment,omitempty"`
	App         string         `json:"app"`
	Image       string         `json:"image"`
	Network     *NetworkConfig `json:"network,omitempty"`
}

type helperRequest struct {
	Config  helperConfig          `json:"config"`
	Request provision.Request     `json:"request,omitempty"`
	List    provision.ListOptions `json:"list,omitempty"`
	ID      string                `json:"id,omitempty"`
}

func (d *Driver) config() helperConfig {
	return helperConfig{Environment: d.environment, App: d.app, Image: d.image, Network: d.network}
}

func normalize(machine provision.Machine, request provision.Request) provision.Machine {
	machine.Provider = "modal"
	machine.Name = request.Name
	machine.Tenant = request.Tenant
	machine.Region = request.Region
	machine.Labels = provision.CloneRequest(request).Labels
	return machine
}

func safeID(value string) bool {
	return value != "" && !strings.ContainsAny(value, "/\\\x00\r\n")
}

func cloneNetwork(input *NetworkConfig) (*NetworkConfig, error) {
	if input == nil {
		return nil, nil
	}
	out := *input
	out.OutboundCIDRAllowlist = append([]string(nil), input.OutboundCIDRAllowlist...)
	out.OutboundDomainAllowlist = append([]string(nil), input.OutboundDomainAllowlist...)
	out.InboundCIDRAllowlist = append([]string(nil), input.InboundCIDRAllowlist...)
	values := append(append(append([]string(nil), out.OutboundCIDRAllowlist...), out.OutboundDomainAllowlist...), out.InboundCIDRAllowlist...)
	for _, value := range values {
		if strings.TrimSpace(value) == "" || strings.ContainsAny(value, "\x00\r\n") {
			return nil, errors.New("modal: network destination is invalid")
		}
	}
	return &out, nil
}

var _ provision.Driver = (*Driver)(nil)
