// Package e2b provisions whole Remount nodes as E2B sandboxes.
package e2b

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/provision/internal/providerutil"
)

const defaultEndpoint = "https://api.e2b.app"

// Config configures an E2B template whose startup reads the REMOUNT_* env and
// execs the candidate node binary. Templates, rather than the API response,
// own that boot-time behavior.
type Config struct {
	Endpoint        string
	APIKey          string
	Template        string
	HTTPClient      *http.Client
	TimeoutSeconds  int
	ProviderRetries int
	Network         *NetworkConfig
}

// NetworkConfig is optional defense-in-depth E2B egress policy. It never
// contributes to the workspace backend's advertised security capabilities.
type NetworkConfig struct {
	AllowPublicTraffic *bool    `json:"allowPublicTraffic,omitempty"`
	AllowOut           []string `json:"allowOut,omitempty"`
	DenyOut            []string `json:"denyOut,omitempty"`
}

// Driver provisions E2B sandboxes through its public REST API.
type Driver struct {
	api      *providerutil.HTTP
	template string
	timeout  int
	network  *NetworkConfig
	mu       sync.Mutex
}

// New validates config without contacting E2B.
func New(config Config) (*Driver, error) {
	if config.Endpoint == "" {
		config.Endpoint = defaultEndpoint
	}
	if config.APIKey == "" || config.Template == "" {
		return nil, fmt.Errorf("%w: E2B API key and bootstrap template are required", provision.ErrUnavailable)
	}
	timeout := config.TimeoutSeconds
	if timeout == 0 {
		timeout = 3600
	}
	if timeout < 60 || timeout > 24*60*60 {
		return nil, errors.New("e2b: timeout must be between 60 and 86400 seconds")
	}
	headers := make(http.Header)
	headers.Set("X-API-Key", config.APIKey)
	api, err := providerutil.NewHTTP(providerutil.HTTPOptions{
		Provider: "e2b", Endpoint: config.Endpoint, Headers: headers,
		Client: config.HTTPClient, Retries: config.ProviderRetries,
	})
	if err != nil {
		return nil, err
	}
	network, err := cloneNetwork(config.Network)
	if err != nil {
		return nil, err
	}
	return &Driver{api: api, template: config.Template, timeout: timeout, network: network}, nil
}

// Name implements provision.Driver.
func (*Driver) Name() string { return "e2b" }

// Create starts one sandbox. Region and size are rejected because the public
// sandbox create schema does not provide those placement controls.
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
		return provision.Machine{}, fmt.Errorf("%w: E2B sandbox API does not support explicit region or size", provision.ErrUnavailable)
	}
	if existing, ok, err := d.find(ctx, request.Name); err != nil {
		return provision.Machine{}, err
	} else if ok {
		if existing.Tenant != request.Tenant {
			return provision.Machine{}, errors.New("e2b: sandbox name belongs to another tenant")
		}
		return existing, nil
	}
	metadata := metadataFor(request)
	body := newSandbox{
		TemplateID: d.template, Timeout: d.timeout, Secure: true,
		Metadata: metadata, Network: d.network,
		EnvVars: map[string]string{
			"REMOUNT_ENROLL_TOKEN": request.Bootstrap.EnrollmentToken,
			"REMOUNT_SERVER":       request.Bootstrap.ServerURL,
			"REMOUNT_BINARY_URL":   request.Bootstrap.BinaryURL,
			"REMOUNT_BACKEND":      request.Bootstrap.Backend,
			"REMOUNT_DATA_DIR":     request.Bootstrap.DataDir,
			"REMOUNT_NODE_ID":      request.Bootstrap.NodeID,
		},
	}
	var response sandbox
	if err := d.api.JSON(ctx, http.MethodPost, "/sandboxes", nil, body, &response, http.StatusCreated); err != nil {
		if found, ok, findErr := d.find(ctx, request.Name); findErr == nil && ok {
			return found, nil
		}
		return provision.Machine{}, err
	}
	machine := response.machine()
	if machine.ID == "" {
		return provision.Machine{}, errors.New("e2b: create response has no sandbox id")
	}
	machine.Name = request.Name
	machine.Tenant = request.Tenant
	machine.Labels = request.Labels
	return machine, nil
}

// Destroy kills a sandbox. A missing sandbox is already destroyed.
func (d *Driver) Destroy(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !safeID(id) {
		return errors.New("e2b: sandbox id is invalid")
	}
	return d.api.JSON(ctx, http.MethodDelete, "/sandboxes/"+url.PathEscape(id), nil, nil, nil, http.StatusNoContent, http.StatusNotFound)
}

// List returns running sandboxes filtered again locally by tenant and pool.
func (d *Driver) List(ctx context.Context, options provision.ListOptions) ([]provision.Machine, error) {
	filters := url.Values{"remount_managed": {"true"}}
	if options.Tenant != "" {
		filters.Set("remount_tenant", options.Tenant)
	}
	if options.Pool != "" {
		filters.Set("remount_pool", options.Pool)
	}
	response, err := d.listRaw(ctx, filters)
	if err != nil {
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

func (d *Driver) find(ctx context.Context, name string) (provision.Machine, bool, error) {
	filters := url.Values{"remount_managed": {"true"}, "remount_name": {name}}
	response, err := d.listRaw(ctx, filters)
	if err != nil {
		return provision.Machine{}, false, err
	}
	for _, raw := range response {
		machine := raw.machine()
		if machine.Name == name {
			return machine, true, nil
		}
	}
	return provision.Machine{}, false, nil
}

func (d *Driver) listRaw(ctx context.Context, filters url.Values) ([]sandbox, error) {
	const maxPages = 100
	query := url.Values{"metadata": {filters.Encode()}, "limit": {"100"}}
	var all []sandbox
	for page := 0; page < maxPages; page++ {
		var response []sandbox
		headers, err := d.api.JSONWithHeaders(ctx, http.MethodGet, "/v2/sandboxes", query, nil, &response, http.StatusOK)
		if err != nil {
			return nil, err
		}
		all = append(all, response...)
		next := headers.Get("X-Next-Token")
		if next == "" {
			return all, nil
		}
		query.Set("nextToken", next)
	}
	return nil, errors.New("e2b: sandbox inventory exceeds pagination bound")
}

type newSandbox struct {
	TemplateID string            `json:"templateID"`
	Timeout    int               `json:"timeout"`
	Secure     bool              `json:"secure"`
	Metadata   map[string]string `json:"metadata"`
	EnvVars    map[string]string `json:"envVars"`
	Network    *NetworkConfig    `json:"network,omitempty"`
}

type sandbox struct {
	SandboxID string            `json:"sandboxID"`
	StartedAt time.Time         `json:"startedAt"`
	State     string            `json:"state"`
	Metadata  map[string]string `json:"metadata"`
}

func (s sandbox) machine() provision.Machine {
	labels := make(map[string]string)
	for key, value := range s.Metadata {
		if strings.HasPrefix(key, "remount_label_") {
			labels[strings.TrimPrefix(key, "remount_label_")] = value
		}
	}
	if s.Metadata["remount_pool"] != "" {
		labels[provision.PoolLabel] = s.Metadata["remount_pool"]
	}
	state := s.State
	if state == "" {
		state = "running"
	}
	return provision.Machine{ID: s.SandboxID, Name: s.Metadata["remount_name"], Provider: "e2b", Tenant: s.Metadata["remount_tenant"], State: state, Labels: labels, CreatedAt: s.StartedAt}
}

func metadataFor(request provision.Request) map[string]string {
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
	return metadata
}

func safeID(value string) bool {
	return value != "" && !strings.ContainsAny(value, "/\\\x00\r\n")
}

func cloneNetwork(input *NetworkConfig) (*NetworkConfig, error) {
	if input == nil {
		return nil, nil
	}
	out := *input
	if input.AllowPublicTraffic != nil {
		value := *input.AllowPublicTraffic
		out.AllowPublicTraffic = &value
	}
	out.AllowOut = append([]string(nil), input.AllowOut...)
	out.DenyOut = append([]string(nil), input.DenyOut...)
	for _, value := range append(append([]string(nil), out.AllowOut...), out.DenyOut...) {
		if strings.TrimSpace(value) == "" || strings.ContainsAny(value, "\x00\r\n") {
			return nil, errors.New("e2b: network destination is invalid")
		}
	}
	return &out, nil
}

var _ provision.Driver = (*Driver)(nil)
