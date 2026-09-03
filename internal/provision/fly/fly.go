// Package fly provisions whole Remount nodes with the Fly Machines API.
package fly

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/provision/internal/providerutil"
)

const defaultEndpoint = "https://api.machines.dev"

// SecretStore stages per-machine secrets in Fly's encrypted app vault. A
// Machine references the secret by name; the plaintext never enters its config.
type SecretStore interface {
	Stage(context.Context, string, string) error
	Remove(context.Context, string) error
}

// CLISecrets uses `fly secrets import` stdin and a staged unset. The secret
// value is never an argument or part of an error.
type CLISecrets struct {
	Runner providerutil.Runner
	Fly    string
	App    string
	Token  string
}

// Stage implements SecretStore.
func (s CLISecrets) Stage(ctx context.Context, name, value string) error {
	if strings.ContainsAny(value, "\x00\r\n") {
		return errors.New("fly: enrollment token cannot be represented by secrets import")
	}
	return s.runner().RunDiscard(ctx, providerutil.Command{
		Executable: s.executable(), Args: []string{"secrets", "import", "--stage", "--app", s.App},
		Stdin: []byte(name + "=" + value + "\n"), Env: map[string]string{"FLY_API_TOKEN": s.Token},
	})
}

// Remove implements SecretStore.
func (s CLISecrets) Remove(ctx context.Context, name string) error {
	return s.runner().RunDiscard(ctx, providerutil.Command{
		Executable: s.executable(), Args: []string{"secrets", "unset", "--stage", "--app", s.App, name},
		Env: map[string]string{"FLY_API_TOKEN": s.Token},
	})
}

func (s CLISecrets) runner() discardRunner {
	runner := s.Runner
	if runner == nil {
		runner = providerutil.OSRunner{}
	}
	return discardRunner{Runner: runner}
}

func (s CLISecrets) executable() string {
	if s.Fly != "" {
		return s.Fly
	}
	return "fly"
}

type discardRunner struct{ providerutil.Runner }

func (r discardRunner) RunDiscard(ctx context.Context, command providerutil.Command) error {
	_, err := r.Run(ctx, command)
	return err
}

// Config configures one Fly app as a tenant-pool machine namespace.
type Config struct {
	Endpoint        string
	Token           string
	App             string
	Image           string
	BootstrapPath   string
	HTTPClient      *http.Client
	Secrets         SecretStore
	WaitTimeout     time.Duration
	ProviderRetries int
}

// Driver provisions Fly Machines. Secret staging is serialized because Fly
// app secret releases are app-wide state even though names are per machine.
type Driver struct {
	api       *providerutil.HTTP
	app       string
	image     string
	bootstrap string
	secrets   SecretStore
	wait      time.Duration
	mu        sync.Mutex
}

// New validates config. A SecretStore is mandatory: plain Machine env is
// provider-readable configuration and is not accepted for enrollment tokens.
func New(config Config) (*Driver, error) {
	if config.Endpoint == "" {
		config.Endpoint = defaultEndpoint
	}
	if config.Token == "" || config.App == "" || config.Image == "" {
		return nil, fmt.Errorf("%w: Fly token, app, and image are required", provision.ErrUnavailable)
	}
	if config.Secrets == nil {
		return nil, fmt.Errorf("%w: Fly encrypted secret staging is required", provision.ErrUnavailable)
	}
	bootstrap := config.BootstrapPath
	if bootstrap == "" {
		bootstrap = "/usr/local/bin/remount-provision-bootstrap"
	}
	if !safeAbsolute(bootstrap) {
		return nil, errors.New("fly: bootstrap path must be a shell-free absolute path")
	}
	wait := config.WaitTimeout
	if wait == 0 {
		wait = 2 * time.Minute
	}
	if wait < time.Second || wait > 10*time.Minute {
		return nil, errors.New("fly: wait timeout must be between 1s and 10m")
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+config.Token)
	api, err := providerutil.NewHTTP(providerutil.HTTPOptions{
		Provider: "fly", Endpoint: config.Endpoint, Headers: headers,
		Client: config.HTTPClient, Retries: config.ProviderRetries,
	})
	if err != nil {
		return nil, err
	}
	return &Driver{api: api, app: config.App, image: config.Image, bootstrap: bootstrap, secrets: config.Secrets, wait: wait}, nil
}

// Name implements provision.Driver.
func (*Driver) Name() string { return "fly" }

// Create stages the one-time token, starts one Machine, waits for Fly's
// started state, and stages removal of the vault entry before returning.
func (d *Driver) Create(ctx context.Context, request provision.Request) (provision.Machine, error) {
	request = provision.CloneRequest(request)
	if err := provision.ValidateSecretBoundary(request); err != nil {
		return provision.Machine{}, err
	}
	if err := request.Validate(); err != nil {
		return provision.Machine{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if existing, ok, err := d.find(ctx, request.Name); err != nil {
		return provision.Machine{}, err
	} else if ok {
		if existing.Tenant != request.Tenant {
			return provision.Machine{}, errors.New("fly: machine name belongs to another tenant")
		}
		if err := d.waitStarted(ctx, existing.ID); err != nil {
			return provision.Machine{}, err
		}
		existing.State = "started"
		return existing.Machine, nil
	}
	secretName := enrollmentSecretName(request.Name, request.Bootstrap.EnrollmentToken)
	if err := d.secrets.Stage(ctx, secretName, request.Bootstrap.EnrollmentToken); err != nil {
		return provision.Machine{}, errors.New("fly: stage enrollment secret failed")
	}
	secretRemoved := false
	defer func() {
		if !secretRemoved {
			_ = d.secrets.Remove(context.WithoutCancel(ctx), secretName)
		}
	}()
	body, err := d.createBody(request, secretName)
	if err != nil {
		return provision.Machine{}, err
	}
	var response flyMachine
	err = d.api.JSON(ctx, http.MethodPost, d.machinePath(), nil, body, &response, http.StatusOK, http.StatusCreated)
	if err != nil {
		if found, ok, findErr := d.find(ctx, request.Name); findErr == nil && ok {
			response = found.raw
		} else {
			return provision.Machine{}, err
		}
	}
	machine := response.machine()
	if machine.ID == "" {
		return provision.Machine{}, errors.New("fly: create response has no machine id")
	}
	if err := d.waitStarted(ctx, machine.ID); err != nil {
		return provision.Machine{}, err
	}
	if err := d.secrets.Remove(context.WithoutCancel(ctx), secretName); err != nil {
		return provision.Machine{}, errors.New("fly: remove staged enrollment secret failed")
	}
	secretRemoved = true
	machine.State = "started"
	return machine, nil
}

// Destroy permanently deletes a Machine. Missing machines are already gone.
func (d *Driver) Destroy(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !safeID(id) {
		return errors.New("fly: machine id is invalid")
	}
	err := d.api.JSON(ctx, http.MethodDelete, d.machinePath()+"/"+url.PathEscape(id), url.Values{"force": {"true"}}, nil, nil, http.StatusOK, http.StatusNoContent, http.StatusNotFound)
	return err
}

// List returns only machines within the requested tenant and pool.
func (d *Driver) List(ctx context.Context, options provision.ListOptions) ([]provision.Machine, error) {
	query := make(url.Values)
	query.Set("metadata.remount_managed", "true")
	if options.Tenant != "" {
		query.Set("metadata.remount_tenant", options.Tenant)
	}
	if options.Pool != "" {
		query.Set("metadata.remount_pool", options.Pool)
	}
	var response []flyMachine
	if err := d.api.JSON(ctx, http.MethodGet, d.machinePath(), query, nil, &response, http.StatusOK); err != nil {
		return nil, err
	}
	out := make([]provision.Machine, 0, len(response))
	for _, raw := range response {
		machine := raw.machine()
		if options.Tenant != "" && machine.Tenant != options.Tenant || options.Pool != "" && machine.Labels[provision.PoolLabel] != options.Pool {
			continue
		}
		out = append(out, provision.CloneMachine(machine))
	}
	provision.SortMachines(out)
	return out, nil
}

type foundMachine struct {
	provision.Machine
	raw flyMachine
}

func (d *Driver) find(ctx context.Context, name string) (foundMachine, bool, error) {
	machines, err := d.List(ctx, provision.ListOptions{})
	if err != nil {
		return foundMachine{}, false, err
	}
	for _, machine := range machines {
		if machine.Name == name {
			return foundMachine{Machine: machine, raw: flyMachineFrom(machine)}, true, nil
		}
	}
	return foundMachine{}, false, nil
}

func (d *Driver) waitStarted(ctx context.Context, id string) error {
	timeoutSeconds := int(d.wait / time.Second)
	query := url.Values{"state": {"started"}, "timeout": {strconv.Itoa(timeoutSeconds)}}
	return d.api.JSON(ctx, http.MethodGet, d.machinePath()+"/"+url.PathEscape(id)+"/wait", query, nil, nil, http.StatusOK)
}

func (d *Driver) machinePath() string { return "/v1/apps/" + url.PathEscape(d.app) + "/machines" }

type flyCreate struct {
	Name   string    `json:"name"`
	Region string    `json:"region,omitempty"`
	Config flyConfig `json:"config"`
}

type flyConfig struct {
	Image     string            `json:"image"`
	Env       map[string]string `json:"env"`
	Metadata  map[string]string `json:"metadata"`
	Processes []flyProcess      `json:"processes"`
	Guest     *flyGuest         `json:"guest,omitempty"`
	Restart   flyRestart        `json:"restart"`
}

type flyProcess struct {
	Entrypoint []string    `json:"entrypoint"`
	Secrets    []flySecret `json:"secrets"`
}

type flySecret struct {
	EnvVar string `json:"env_var"`
	Name   string `json:"name"`
}

type flyGuest struct {
	CPUKind  string `json:"cpu_kind"`
	CPUs     int    `json:"cpus"`
	MemoryMB int    `json:"memory_mb"`
}

type flyRestart struct {
	Policy string `json:"policy"`
}

func (d *Driver) createBody(request provision.Request, secretName string) (flyCreate, error) {
	guest, err := parseSize(request.Size)
	if err != nil {
		return flyCreate{}, err
	}
	metadata := map[string]string{
		"remount_managed": "true", "remount_name": request.Name,
		"remount_tenant": request.Tenant, "remount_pool": request.Labels[provision.PoolLabel],
	}
	for key, value := range request.Labels {
		if key == provision.PoolLabel {
			continue
		}
		metadata["remount_label_"+key] = value
	}
	env := map[string]string{
		"REMOUNT_SERVER": request.Bootstrap.ServerURL, "REMOUNT_BINARY_URL": request.Bootstrap.BinaryURL,
		"REMOUNT_BACKEND": request.Bootstrap.Backend, "REMOUNT_DATA_DIR": request.Bootstrap.DataDir,
	}
	return flyCreate{Name: request.Name, Region: request.Region, Config: flyConfig{
		Image: d.image, Env: env, Metadata: metadata, Guest: guest, Restart: flyRestart{Policy: "always"},
		Processes: []flyProcess{{Entrypoint: []string{d.bootstrap}, Secrets: []flySecret{{EnvVar: "REMOUNT_ENROLL_TOKEN", Name: secretName}}}},
	}}, nil
}

type flyMachine struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	State     string    `json:"state"`
	Region    string    `json:"region"`
	CreatedAt time.Time `json:"created_at"`
	Config    struct {
		Metadata map[string]string `json:"metadata"`
	} `json:"config"`
}

func (m flyMachine) machine() provision.Machine {
	labels := make(map[string]string)
	for key, value := range m.Config.Metadata {
		if strings.HasPrefix(key, "remount_label_") {
			labels[strings.TrimPrefix(key, "remount_label_")] = value
		}
	}
	if m.Config.Metadata["remount_pool"] != "" {
		labels[provision.PoolLabel] = m.Config.Metadata["remount_pool"]
	}
	return provision.Machine{ID: m.ID, Name: first(m.Name, m.Config.Metadata["remount_name"]), Provider: "fly", Tenant: m.Config.Metadata["remount_tenant"], Region: m.Region, State: m.State, Labels: labels, CreatedAt: m.CreatedAt}
}

func flyMachineFrom(machine provision.Machine) flyMachine {
	var raw flyMachine
	raw.ID, raw.Name, raw.State, raw.Region, raw.CreatedAt = machine.ID, machine.Name, machine.State, machine.Region, machine.CreatedAt
	raw.Config.Metadata = map[string]string{"remount_tenant": machine.Tenant, "remount_pool": machine.Labels[provision.PoolLabel], "remount_name": machine.Name}
	return raw
}

func enrollmentSecretName(name, token string) string {
	sum := sha256.Sum256([]byte(name + "\x00" + token))
	return "REMOUNT_ENROLL_" + strings.ToUpper(hex.EncodeToString(sum[:8]))
}

func parseSize(size string) (*flyGuest, error) {
	if size == "" {
		return nil, nil
	}
	cpuKind := ""
	count := ""
	if strings.HasPrefix(size, "shared-cpu-") {
		cpuKind, count = "shared", strings.TrimPrefix(size, "shared-cpu-")
	} else if strings.HasPrefix(size, "performance-") {
		cpuKind, count = "performance", strings.TrimPrefix(size, "performance-")
	} else {
		return nil, fmt.Errorf("%w: unsupported Fly size %q", provision.ErrUnavailable, size)
	}
	if !strings.HasSuffix(count, "x") {
		return nil, fmt.Errorf("%w: unsupported Fly size %q", provision.ErrUnavailable, size)
	}
	cpus, err := strconv.Atoi(strings.TrimSuffix(count, "x"))
	allowed := map[int]bool{1: true, 2: true, 4: true, 8: true, 16: true}
	if cpuKind == "shared" {
		allowed[6] = true
	}
	if err != nil || !allowed[cpus] {
		return nil, fmt.Errorf("%w: unsupported Fly size %q", provision.ErrUnavailable, size)
	}
	memoryPerCPU := 256
	if cpuKind == "performance" {
		memoryPerCPU = 2048
	}
	return &flyGuest{CPUKind: cpuKind, CPUs: cpus, MemoryMB: memoryPerCPU * cpus}, nil
}

func safeID(value string) bool {
	return value != "" && !strings.ContainsAny(value, "/\\\x00\r\n")
}

func safeAbsolute(value string) bool {
	return strings.HasPrefix(value, "/") && !strings.ContainsAny(value, " \t\r\n\x00'\"`$\\")
}

func first(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var _ provision.Driver = (*Driver)(nil)
