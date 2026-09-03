// Package ssh enrolls a whole Remount node on a dedicated BYO SSH host.
package ssh

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/provision/internal/providerutil"
)

// Config pins one host to one tenant. The remote helper is pre-installed by
// the host owner and accepts bounded JSON on stdin.
type Config struct {
	SSH            string
	Runner         providerutil.Runner
	Host           string
	Port           int
	User           string
	IdentityFile   string
	KnownHostsFile string
	RemoteHelper   string
	Tenant         string
	Pool           string
	Region         string
	Size           string
	MaxOutput      int
}

// Driver manages one pre-existing host; it never creates or deletes the host.
// Destroy stops and removes only the Remount node installation through the
// remote helper.
type Driver struct {
	helper providerutil.Helper
	tenant string
	pool   string
	host   string
	region string
	size   string
	mu     sync.Mutex
}

// New validates strict host-key and identity configuration. There is no
// insecure first-use or known-hosts fallback.
func New(config Config) (*Driver, error) {
	if config.SSH == "" {
		config.SSH = "ssh"
	}
	if config.Port == 0 {
		config.Port = 22
	}
	if config.RemoteHelper == "" {
		config.RemoteHelper = "/usr/local/bin/remount-provision-bootstrap"
	}
	if !safeAtom(config.User) || !safeHost(config.Host) || config.Port < 1 || config.Port > 65535 ||
		!safePath(config.IdentityFile) || !safePath(config.KnownHostsFile) || !safeRemotePath(config.RemoteHelper) || config.Tenant == "" {
		return nil, fmt.Errorf("%w: SSH host, tenant, identity, known-hosts, and remote helper must be pinned", provision.ErrUnavailable)
	}
	if config.Runner == nil {
		if _, err := exec.LookPath(config.SSH); err != nil {
			return nil, fmt.Errorf("%w: SSH client executable is not installed", provision.ErrUnavailable)
		}
	}
	host := config.Host
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	target := config.User + "@" + host
	args := []string{
		"-F", "/dev/null",
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=" + config.KnownHostsFile,
		"-o", "GlobalKnownHostsFile=/dev/null",
		"-o", "IdentitiesOnly=yes",
		"-o", "IdentityAgent=none",
		"-o", "PreferredAuthentications=publickey",
		"-o", "PasswordAuthentication=no",
		"-o", "KbdInteractiveAuthentication=no",
		"-o", "ClearAllForwardings=yes",
		"-o", "ForwardAgent=no",
		"-o", "ForwardX11=no",
		"-o", "PermitLocalCommand=no",
		"-o", "RequestTTY=no",
		"-i", config.IdentityFile,
		"-p", strconv.Itoa(config.Port),
		"--", target, config.RemoteHelper,
	}
	return &Driver{
		helper: providerutil.Helper{Runner: config.Runner, Executable: config.SSH, PrefixArgs: args, MaxOutput: config.MaxOutput},
		tenant: config.Tenant, pool: config.Pool, host: config.Host, region: config.Region, size: config.Size,
	}, nil
}

// Name implements provision.Driver.
func (*Driver) Name() string { return "ssh" }

// Create idempotently installs and starts a node through the pinned helper.
// All request data, including enrollment, travels over SSH stdin.
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
	if request.Tenant != d.tenant || request.Labels[provision.PoolLabel] != d.pool {
		return provision.Machine{}, errors.New("ssh: request is outside the host's tenant or pool")
	}
	if request.Region != "" && request.Region != d.region || request.Size != "" && request.Size != d.size {
		return provision.Machine{}, fmt.Errorf("%w: SSH host does not match requested placement", provision.ErrUnavailable)
	}
	existing, err := d.List(ctx, provision.ListOptions{Tenant: d.tenant, Pool: d.pool})
	if err != nil {
		return provision.Machine{}, err
	}
	if len(existing) != 0 {
		if existing[0].Name != request.Name {
			return provision.Machine{}, errors.New("ssh: host is already assigned to another node")
		}
		return existing[0], nil
	}
	var response provision.Machine
	if err := d.helper.Call(ctx, "create", helperRequest{Request: request}, &response); err != nil {
		return provision.Machine{}, err
	}
	response.ID = d.host
	response.Name = request.Name
	response.Provider = "ssh"
	response.Tenant = d.tenant
	response.Region = d.region
	response.Labels = request.Labels
	return provision.CloneMachine(response), nil
}

// Destroy stops and removes the Remount node installation. It deliberately
// does not destroy the BYO host.
func (d *Driver) Destroy(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id != d.host {
		return provision.ErrNotFound
	}
	return d.helper.Call(ctx, "destroy", helperRequest{ID: id}, nil)
}

// List asks the pinned host for its node state and re-applies tenant/pool
// filters locally. Host unavailability is an error, never an empty inventory.
func (d *Driver) List(ctx context.Context, options provision.ListOptions) ([]provision.Machine, error) {
	if options.Tenant != "" && options.Tenant != d.tenant || options.Pool != "" && options.Pool != d.pool {
		return nil, nil
	}
	var response struct {
		Present bool              `json:"present"`
		Machine provision.Machine `json:"machine"`
	}
	if err := d.helper.Call(ctx, "list", helperRequest{List: options}, &response); err != nil {
		return nil, err
	}
	if !response.Present {
		return nil, nil
	}
	response.Machine.ID = d.host
	response.Machine.Provider = "ssh"
	response.Machine.Tenant = d.tenant
	response.Machine.Region = d.region
	if response.Machine.Labels == nil {
		response.Machine.Labels = map[string]string{}
	}
	response.Machine.Labels[provision.PoolLabel] = d.pool
	return []provision.Machine{provision.CloneMachine(response.Machine)}, nil
}

type helperRequest struct {
	Request provision.Request     `json:"request,omitempty"`
	List    provision.ListOptions `json:"list,omitempty"`
	ID      string                `json:"id,omitempty"`
}

func safeAtom(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("._-", char)) {
			return false
		}
	}
	return true
}

func safeHost(value string) bool {
	if net.ParseIP(value) != nil {
		return true
	}
	if len(value) == 0 || len(value) > 253 {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' || !safeAtom(label) {
			return false
		}
	}
	return true
}

func safePath(value string) bool {
	return strings.HasPrefix(value, "/") && !strings.ContainsAny(value, "\x00\r\n")
}

func safeRemotePath(value string) bool {
	return strings.HasPrefix(value, "/") && !strings.ContainsAny(value, " \t\x00\r\n'\"`$\\;&|<>(){}[]*?!~")
}

var _ provision.Driver = (*Driver)(nil)
