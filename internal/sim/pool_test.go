package sim

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/pool"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/server"
)

type simEnrollment struct {
	mu       sync.Mutex
	tokens   map[string]control.NodeIdentity
	bindings map[string]struct {
		key      []byte
		identity control.NodeIdentity
	}
}

func newSimEnrollment() *simEnrollment {
	return &simEnrollment{tokens: map[string]control.NodeIdentity{}, bindings: map[string]struct {
		key      []byte
		identity control.NodeIdentity
	}{}}
}

func (s *simEnrollment) Issue(_ context.Context, poolName, tenant string, _ time.Duration) (string, error) {
	return s.IssueWithLabels(context.Background(), poolName, tenant, nil, time.Minute)
}

func (s *simEnrollment) IssueWithLabels(_ context.Context, poolName, tenant string, labels map[string]string, _ time.Duration) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	token := "enroll_" + poolName + "_" + tenant
	s.tokens[token] = control.NodeIdentity{Tenant: tenant, Pool: poolName, Token: "tok", Labels: cloneSimLabels(labels)}
	return token, nil
}

func (s *simEnrollment) AuthenticateNode(_ context.Context, nodeID, token string, key []byte) (control.NodeIdentity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if binding, ok := s.bindings[nodeID]; ok {
		if !bytes.Equal(binding.key, key) {
			return control.NodeIdentity{}, errors.New("node key changed")
		}
		return binding.identity, nil
	}
	identity, ok := s.tokens[token]
	if !ok || len(key) != ed25519.PublicKeySize {
		return control.NodeIdentity{}, errors.New("invalid enrollment")
	}
	delete(s.tokens, token)
	identity.Fresh = true
	if identity.Labels == nil {
		identity.Labels = map[string]string{}
	}
	identity.Labels[provision.PoolLabel], identity.Labels[provision.NodeLabel] = identity.Pool, nodeID
	s.bindings[nodeID] = struct {
		key      []byte
		identity control.NodeIdentity
	}{key: append([]byte(nil), key...), identity: identity}
	return identity, nil
}

func cloneSimLabels(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))
	for key, value := range labels {
		out[key] = value
	}
	return out
}

type simProvisioner struct {
	mu        sync.Mutex
	machines  map[string]provision.Machine
	cancels   map[string]context.CancelFunc
	startNode func(provision.Request) (context.CancelFunc, error)
}

func (s *simProvisioner) Name() string { return "fake" }

func (s *simProvisioner) Create(_ context.Context, request provision.Request) (provision.Machine, error) {
	if err := request.Validate(); err != nil {
		return provision.Machine{}, err
	}
	machine := provision.Machine{ID: "vm-" + request.Bootstrap.NodeID, Name: request.Name, Provider: s.Name(),
		Tenant: request.Tenant, Labels: provision.CloneRequest(request).Labels, State: "running", CreatedAt: time.Now()}
	s.mu.Lock()
	s.machines[machine.ID] = machine
	s.mu.Unlock()
	cancel, err := s.startNode(request)
	if err != nil {
		s.mu.Lock()
		delete(s.machines, machine.ID)
		s.mu.Unlock()
		return provision.Machine{}, err
	}
	s.mu.Lock()
	s.cancels[machine.ID] = cancel
	s.mu.Unlock()
	return machine, nil
}

func (s *simProvisioner) Destroy(_ context.Context, id string) error {
	s.mu.Lock()
	cancel, ok := s.cancels[id]
	if ok {
		delete(s.cancels, id)
		delete(s.machines, id)
	}
	s.mu.Unlock()
	if !ok {
		return provision.ErrNotFound
	}
	cancel()
	return nil
}

func (s *simProvisioner) List(_ context.Context, options provision.ListOptions) ([]provision.Machine, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []provision.Machine
	for _, machine := range s.machines {
		if machine.Tenant == options.Tenant && machine.Labels[provision.PoolLabel] == options.Pool {
			out = append(out, provision.CloneMachine(machine))
		}
	}
	provision.SortMachines(out)
	return out, nil
}

func TestE17PoolClaimScalesUpAndIdleScalesDown(t *testing.T) {
	enrollment := newSimEnrollment()
	provider := &simProvisioner{machines: map[string]provision.Machine{}, cancels: map[string]context.CancelFunc{}}
	var w *world
	w = newWorldWith(t, func(options *server.Options) {
		options.NodeAuthenticator = enrollment
		options.PoolEnrollmentSource = enrollment
		options.ProvisionDrivers = []provision.Driver{provider}
		options.PoolBootstrap = control.PoolBootstrap{ServerURL: "http://control.invalid", BinaryURL: "http://control.invalid/remount", DataDir: "/var/lib/remount"}
		options.PoolOptions = pool.Options{MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, PendingTimeout: time.Second}
	})
	nodeRoot := t.TempDir()
	provider.startNode = func(request provision.Request) (context.CancelFunc, error) {
		dataDir := filepath.Join(nodeRoot, request.Bootstrap.NodeID)
		if err := os.MkdirAll(dataDir, 0o700); err != nil {
			return nil, err
		}
		n, err := node.New(node.Options{DataDir: dataDir, ID: request.Bootstrap.NodeID,
			Dialer: w.dialer(request.Bootstrap.NodeID), Token: request.Bootstrap.EnrollmentToken,
			ArtifactURL: w.http.URL + "/v1/artifacts", Allow: []string{"127.0.0.1"}, AllowPrivate: []string{"127.0.0.1", "localhost"}})
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithCancel(w.ctx)
		go n.Run(ctx)
		select {
		case <-n.Online():
			return cancel, nil
		case <-time.After(5 * time.Second):
			cancel()
			return nil, errors.New("provisioned node did not enroll")
		}
	}

	c := w.client("operator")
	ctx := ctxT(t, 30*time.Second)
	if _, err := c.CreatePool(ctx, proto.PoolSpec{Name: "iad", Vendor: "fake", Min: 0, Max: 5,
		Backend: "process", Labels: map[string]string{"region": "iad"}, IdleScaleDownMilli: 50}); err != nil {
		t.Fatal(err)
	}
	created, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: "pool-demand", Requires: proto.Requires{Backend: "process"},
		Placement: proto.Placement{Allow: map[string]string{"region": "iad"}}})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := c.WaitClaimed(ctx, created.ID)
	if err != nil || claimed.Node == "" {
		t.Fatalf("workspace claim = %+v, %v", claimed, err)
	}
	if err := c.DestroyWorkspace(ctx, claimed.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for {
		machines, err := provider.List(ctx, provision.ListOptions{Tenant: "local", Pool: "iad"})
		if err != nil {
			t.Fatal(err)
		}
		if len(machines) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("idle provider inventory did not scale down: %#v", machines)
		}
		time.Sleep(50 * time.Millisecond)
	}
	poolState, err := c.GetPool(ctx, "iad")
	if err != nil || poolState.Current != 0 {
		t.Fatalf("durable pool after scale-down = %+v, %v", poolState, err)
	}
	events, err := c.ReadEvents(ctx, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	var up, down bool
	for _, event := range events {
		if event.Type != proto.EvPoolScaled {
			continue
		}
		var payload struct {
			From int `cbor:"from"`
			To   int `cbor:"to"`
		}
		if proto.Unmarshal(event.Payload, &payload) == nil {
			up = up || payload.From == 0 && payload.To == 1
			down = down || payload.From == 1 && payload.To == 0
		}
	}
	if !up || !down {
		t.Fatalf("pool scale events up=%v down=%v", up, down)
	}
}
