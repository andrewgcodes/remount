// Package e2b provisions whole Remount nodes as E2B sandboxes.
package e2b

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/provision/internal/providerutil"
)

const defaultEndpoint = "https://api.e2b.app"

// Config configures an E2B template whose start command waits for the
// bootstrap file this driver delivers, sources it, and execs the node binary.
//
// E2B snapshots a template after its start command has run, and every sandbox
// resumes from that snapshot, so the start command never sees environment
// variables passed at sandbox creation. The driver therefore writes the
// REMOUNT_* bootstrap values to BootstrapPath through the sandbox's envd file
// API immediately after creation; images/e2b holds the template that consumes
// it.
type Config struct {
	Endpoint        string
	APIKey          string
	Template        string
	HTTPClient      *http.Client
	TimeoutSeconds  int
	ProviderRetries int
	Network         *NetworkConfig
	// SandboxDomain is the domain sandboxes are reachable under
	// (port-sandboxID-clientID.<domain>). Empty derives it from Endpoint by
	// dropping a leading "api." label.
	SandboxDomain string
	// EnvdEndpoint, when set, replaces the per-sandbox envd URL for every
	// sandbox. Tests point it at a fake; production leaves it empty.
	EnvdEndpoint string
	// BootstrapPath is where the bootstrap file lands inside the sandbox.
	// Empty means DefaultBootstrapPath.
	BootstrapPath string
	// BootstrapUser is the sandbox user envd writes the file as. Empty means
	// "user", the E2B default template account.
	BootstrapUser string
}

// DefaultBootstrapPath is where the driver writes the bootstrap file unless
// Config.BootstrapPath overrides it. The template's start command polls this
// path, so the two must agree.
const DefaultBootstrapPath = "/home/user/remount-bootstrap.env"

// envdPort is the port envd, the E2B in-sandbox agent, listens on.
const envdPort = 49983

// NetworkConfig is optional defense-in-depth E2B egress policy. It never
// contributes to the workspace backend's advertised security capabilities.
type NetworkConfig struct {
	AllowPublicTraffic *bool    `json:"allowPublicTraffic,omitempty"`
	AllowOut           []string `json:"allowOut,omitempty"`
	DenyOut            []string `json:"denyOut,omitempty"`
}

// Driver provisions E2B sandboxes through its public REST API.
type Driver struct {
	api           *providerutil.HTTP
	envd          *http.Client
	template      string
	timeout       int
	network       *NetworkConfig
	sandboxDomain string
	envdEndpoint  string
	bootstrapPath string
	bootstrapUser string
	mu            sync.Mutex
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
	domain := config.SandboxDomain
	if domain == "" {
		parsed, err := url.Parse(config.Endpoint)
		if err != nil || parsed.Hostname() == "" {
			return nil, errors.New("e2b: endpoint must be a URL")
		}
		domain = strings.TrimPrefix(parsed.Hostname(), "api.")
	}
	bootstrapPath := config.BootstrapPath
	if bootstrapPath == "" {
		bootstrapPath = DefaultBootstrapPath
	}
	if !strings.HasPrefix(bootstrapPath, "/") {
		return nil, errors.New("e2b: bootstrap path must be absolute")
	}
	bootstrapUser := config.BootstrapUser
	if bootstrapUser == "" {
		bootstrapUser = "user"
	}
	envd := config.HTTPClient
	if envd == nil {
		envd = &http.Client{Timeout: 30 * time.Second}
	}
	return &Driver{
		api: api, envd: envd, template: config.Template, timeout: timeout, network: network,
		sandboxDomain: domain, envdEndpoint: config.EnvdEndpoint,
		bootstrapPath: bootstrapPath, bootstrapUser: bootstrapUser,
	}, nil
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
	if err := d.deliverBootstrap(ctx, response, request.Bootstrap); err != nil {
		// A sandbox that never receives its bootstrap never enrolls and would
		// sit as paid, unusable capacity until its timeout; release it now so
		// the reconciler's retry starts clean.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = d.api.JSON(cleanup, http.MethodDelete, "/sandboxes/"+url.PathEscape(machine.ID), nil, nil, nil, http.StatusNoContent, http.StatusNotFound)
		return provision.Machine{}, err
	}
	return machine, nil
}

// deliverBootstrap writes the REMOUNT_* values the node needs to the sandbox
// through envd, the in-sandbox agent, authenticating with the one-time access
// token the create response carried. The token is used for this request and
// never logged, stored, or returned.
func (d *Driver) deliverBootstrap(ctx context.Context, s sandbox, bootstrap provision.Bootstrap) error {
	if s.EnvdAccessToken == "" {
		return errors.New("e2b: create response carried no envd access token; the sandbox was not created secure")
	}
	base := d.envdEndpoint
	if base == "" {
		host := s.SandboxID
		if s.ClientID != "" {
			host += "-" + s.ClientID
		}
		base = fmt.Sprintf("https://%d-%s.%s", envdPort, host, d.sandboxDomain)
	}
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("file", path.Base(d.bootstrapPath))
	if err != nil {
		return fmt.Errorf("e2b: build bootstrap upload: %w", err)
	}
	if _, err := part.Write(bootstrapFile(bootstrap)); err != nil {
		return fmt.Errorf("e2b: build bootstrap upload: %w", err)
	}
	if err := form.Close(); err != nil {
		return fmt.Errorf("e2b: build bootstrap upload: %w", err)
	}
	query := url.Values{"path": {d.bootstrapPath}, "username": {d.bootstrapUser}}
	target := base + "/files?" + query.Encode()
	// The buffer is consumed by the first attempt, so every attempt reads the
	// captured bytes rather than whatever the buffer has left.
	payload := body.Bytes()
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("e2b: deliver bootstrap: %w", ctx.Err())
			case <-time.After(time.Duration(attempt) * 2 * time.Second):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("e2b: build bootstrap upload: %w", err)
		}
		req.Header.Set("Content-Type", form.FormDataContentType())
		req.Header.Set("X-Access-Token", s.EnvdAccessToken)
		resp, err := d.envd.Do(req)
		if err != nil {
			// The transport error may quote the URL, never the token.
			last = fmt.Errorf("e2b: deliver bootstrap: %w", err)
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
			return nil
		}
		last = fmt.Errorf("e2b: deliver bootstrap: envd returned HTTP %d", resp.StatusCode)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusRequestTimeout && resp.StatusCode != http.StatusTooManyRequests {
			return last
		}
	}
	return last
}

// bootstrapFile renders the values as a POSIX shell fragment the template's
// start command sources. Single quotes keep every value literal.
func bootstrapFile(b provision.Bootstrap) []byte {
	var out bytes.Buffer
	for _, kv := range [][2]string{
		{"REMOUNT_SERVER", b.ServerURL},
		{"REMOUNT_ENROLL_TOKEN", b.EnrollmentToken},
		{"REMOUNT_BINARY_URL", b.BinaryURL},
		{"REMOUNT_BACKEND", b.Backend},
		{"REMOUNT_DATA_DIR", b.DataDir},
		{"REMOUNT_NODE_ID", b.NodeID},
	} {
		if kv[1] == "" {
			continue
		}
		fmt.Fprintf(&out, "%s='%s'\n", kv[0], strings.ReplaceAll(kv[1], "'", `'\''`))
	}
	return out.Bytes()
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
	Network    *NetworkConfig    `json:"network,omitempty"`
}

// sandbox is what both the create response and a list item decode into. The
// create response alone carries clientID and envdAccessToken; the token is
// consumed by deliverBootstrap and must never reach a log, an event, or a
// Machine.
type sandbox struct {
	SandboxID       string            `json:"sandboxID"`
	ClientID        string            `json:"clientID"`
	EnvdAccessToken string            `json:"envdAccessToken"`
	StartedAt       time.Time         `json:"startedAt"`
	State           string            `json:"state"`
	Metadata        map[string]string `json:"metadata"`
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
