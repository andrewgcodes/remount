// Package pool reconciles declarative node-pool capacity through infrastructure
// provisioners. It does not place workspaces; control tells it only that pending
// work found no eligible node, and the normal claim queue remains authoritative.
package pool

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/provision"
)

// Spec declares one provider-backed pool. A vendor pool is pinned to one
// tenant; machines from differently scoped pools are never interchangeable.
type Spec struct {
	Name          string
	Tenant        string
	Vendor        string
	Min           int
	Max           int
	Labels        map[string]string
	Backend       string
	IdleScaleDown time.Duration
	Region        string
	Size          string
	Bootstrap     provision.Bootstrap
}

// Node is provider inventory enriched with the control plane's current
// assignment count. IdleSince is meaningful only when Workspaces is zero.
// Workspaces and IdleSince are an observation taken before Reconcile runs;
// Retirement is the authority that makes the observation safe to act on.
type Node struct {
	Machine    provision.Machine
	Workspaces int
	IdleSince  time.Time
	Retirement Retirement
}

// Retirement is the control plane's claim fence for one node. Retire is
// called immediately before the provider destroy: it re-checks the node
// against current assignment state and, only if it is still idle, durably
// blocks new claims and returns true. False means a claim won the race and
// the node is no longer a victim. Release lifts the fence after a definite
// provider failure; an ambiguous result keeps it, because the machine may be
// gone. Retire must be idempotent for a node this pool already fenced.
//
// Implementations run under the control plane's own locks and must not call
// the provider. A Node without a Retirement is never destroyed.
type Retirement interface {
	Retire(context.Context) (bool, error)
	Release(context.Context) error
}

// EnrollmentSource mints one short-lived, single-use node token immediately
// before a provider create. Implementations persist the token hash and its
// audit event before returning the plaintext token.
type EnrollmentSource interface {
	Issue(context.Context, string, string, time.Duration) (string, error)
}

// LabeledEnrollmentSource durably binds scheduler labels to the enrollment
// authority. Control must not trust the labels self-reported by a new node.
type LabeledEnrollmentSource interface {
	IssueWithLabels(context.Context, string, string, map[string]string, time.Duration) (string, error)
}

// Action is the observable outcome control turns into pool.scaled or
// pool.provision_failed. Error text must already be secret-free.
type Action struct {
	Kind    string
	Machine provision.Machine
	From    int
	To      int
	Reason  string
	Error   string
	RetryAt time.Time
}

// Action kinds.
const (
	ActionCreated       = "created"
	ActionDestroyed     = "destroyed"
	ActionCreateFailed  = "create_failed"
	ActionDestroyFailed = "destroy_failed"
	ActionBackoff       = "backoff"
)

// Options configure reconciliation timing. Zero values select conservative
// defaults.
type Options struct {
	Now            func() time.Time
	EnrollmentTTL  time.Duration
	PendingTimeout time.Duration
	MinBackoff     time.Duration
	MaxBackoff     time.Duration
}

// Reconciler serializes irreversible provider actions per pool.
type Reconciler struct {
	drivers map[string]provision.Driver
	tokens  EnrollmentSource
	opts    Options

	mu     sync.Mutex
	states map[string]*state
}

type state struct {
	mu       sync.Mutex
	failures int
	retryAt  time.Time
	pending  map[string]time.Time
}

// New returns a reconciler. Driver names must be unique and token issuance is
// mandatory so a pool can never fall back to a reusable node credential.
func New(drivers []provision.Driver, tokens EnrollmentSource, opts Options) (*Reconciler, error) {
	if tokens == nil {
		return nil, errors.New("pool: enrollment source is required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.EnrollmentTTL <= 0 {
		opts.EnrollmentTTL = 10 * time.Minute
	}
	if opts.PendingTimeout <= 0 {
		opts.PendingTimeout = 2 * time.Minute
	}
	if opts.MinBackoff <= 0 {
		opts.MinBackoff = time.Second
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = 30 * time.Second
	}
	if opts.MaxBackoff < opts.MinBackoff {
		return nil, errors.New("pool: maximum backoff is less than minimum")
	}
	r := &Reconciler{drivers: map[string]provision.Driver{}, tokens: tokens, opts: opts, states: map[string]*state{}}
	for _, driver := range drivers {
		if driver == nil || strings.TrimSpace(driver.Name()) == "" {
			return nil, errors.New("pool: driver name is required")
		}
		if _, exists := r.drivers[driver.Name()]; exists {
			return nil, fmt.Errorf("pool: duplicate driver %q", driver.Name())
		}
		r.drivers[driver.Name()] = driver
	}
	return r, nil
}

// Reconcile moves one pool toward its target. Demand is the count of pending
// workspaces that found no eligible online node; each demand unit admits at
// most one additional machine up to Max. Provider actions for the same pool
// cannot overlap, so concurrent claim notifications cannot overcommit it.
func (r *Reconciler) Reconcile(ctx context.Context, spec Spec, nodes []Node, demand int) ([]Action, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	if demand < 0 {
		return nil, errors.New("pool: demand must not be negative")
	}
	driver := r.drivers[spec.Vendor]
	if driver == nil {
		return nil, fmt.Errorf("pool: vendor %q is not configured", spec.Vendor)
	}
	st := r.poolState(spec.key())
	st.mu.Lock()
	defer st.mu.Unlock()

	now := r.opts.Now()
	owned, err := ownedNodes(spec, nodes)
	if err != nil {
		return nil, err
	}
	visible := make(map[string]struct{}, len(owned))
	for _, node := range owned {
		visible[node.Machine.ID] = struct{}{}
	}
	for id, reservedAt := range st.pending {
		if _, appeared := visible[id]; appeared || !reservedAt.Add(r.opts.PendingTimeout).After(now) {
			delete(st.pending, id)
		}
	}
	effective := len(owned) + len(st.pending)
	if now.Before(st.retryAt) {
		return []Action{{Kind: ActionBackoff, From: effective, To: effective, Reason: "provider_backoff", RetryAt: st.retryAt}}, nil
	}

	desired := spec.Min
	if demand > 0 {
		// Demand is a current count, not an edge-trigger. Provider inventory and
		// pending creates already satisfy that many units; adding demand to the
		// effective count on every tick would over-scale while a node boots.
		desired = min(spec.Max, max(desired, demand))
	}
	if effective < desired {
		return r.scaleUp(ctx, driver, spec, st, effective, desired)
	}
	if len(st.pending) == 0 && len(owned) > spec.Min {
		return r.scaleDown(ctx, driver, spec, st, owned, now)
	}
	st.failures, st.retryAt = 0, time.Time{}
	return nil, nil
}

// Forget drops synchronization/backoff state after a durable pool deletion.
// It must run only after no Reconcile call for the pool remains in flight.
func (r *Reconciler) Forget(spec Spec) {
	r.mu.Lock()
	delete(r.states, spec.key())
	r.mu.Unlock()
}

// Validate rejects specs that could escape a tenant, overcommit capacity, or
// create nodes without an explicit backend and bootstrap contract.
func (s Spec) Validate() error {
	if strings.TrimSpace(s.Name) == "" || strings.ContainsAny(s.Name, "=\x00\n\r") {
		return errors.New("pool: valid name is required")
	}
	if strings.TrimSpace(s.Tenant) == "" {
		return errors.New("pool: tenant is required")
	}
	if strings.TrimSpace(s.Vendor) == "" {
		return errors.New("pool: vendor is required")
	}
	if s.Min < 0 || s.Max <= 0 || s.Min > s.Max {
		return fmt.Errorf("pool: capacity must satisfy 0 <= min <= max, max > 0")
	}
	if s.IdleScaleDown < 0 {
		return errors.New("pool: idle scale-down must not be negative")
	}
	if s.Labels[provision.PoolLabel] != "" && s.Labels[provision.PoolLabel] != s.Name {
		return fmt.Errorf("pool: label %q is reserved", provision.PoolLabel)
	}
	if s.Labels[provision.NodeLabel] != "" {
		return fmt.Errorf("pool: label %q is reserved", provision.NodeLabel)
	}
	b := s.Bootstrap
	b.Backend = s.Backend
	return b.ValidateTemplate()
}

func (r *Reconciler) scaleUp(ctx context.Context, driver provision.Driver, spec Spec, st *state, from, desired int) ([]Action, error) {
	labels := cloneMap(spec.Labels)
	labels[provision.PoolLabel] = spec.Name
	nodeID := ids.New("n")
	labels[provision.NodeLabel] = nodeID
	var token string
	var err error
	if source, ok := r.tokens.(LabeledEnrollmentSource); ok {
		token, err = source.IssueWithLabels(ctx, spec.Name, spec.Tenant, labels, r.opts.EnrollmentTTL)
	} else {
		token, err = r.tokens.Issue(ctx, spec.Name, spec.Tenant, r.opts.EnrollmentTTL)
	}
	if err != nil {
		return r.failed(st, ActionCreateFailed, from, "enrollment", err)
	}
	bootstrap := spec.Bootstrap
	bootstrap.Backend = spec.Backend
	bootstrap.EnrollmentToken = token
	bootstrap.NodeID = nodeID
	req := provision.Request{
		Name:   spec.Name + "-" + strings.TrimPrefix(nodeID, "n_"),
		Tenant: spec.Tenant, Region: spec.Region, Size: spec.Size, Labels: labels, Bootstrap: bootstrap,
	}
	machine, err := driver.Create(ctx, req)
	if err != nil {
		return r.failed(st, ActionCreateFailed, from, "demand", err)
	}
	st.failures, st.retryAt = 0, time.Time{}
	machine = provision.CloneMachine(machine)
	if st.pending == nil {
		st.pending = map[string]time.Time{}
	}
	st.pending[machine.ID] = r.opts.Now()
	return []Action{{Kind: ActionCreated, Machine: machine, From: from, To: from + 1, Reason: "demand"}}, nil
}

func (r *Reconciler) scaleDown(ctx context.Context, driver provision.Driver, spec Spec, st *state, nodes []Node, now time.Time) ([]Action, error) {
	if spec.IdleScaleDown == 0 {
		return nil, nil
	}
	candidates := make([]Node, 0, len(nodes))
	for _, node := range nodes {
		if node.Workspaces == 0 && !node.IdleSince.IsZero() && !node.IdleSince.Add(spec.IdleScaleDown).After(now) {
			candidates = append(candidates, node)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	slices.SortFunc(candidates, func(a, b Node) int {
		if d := a.IdleSince.Compare(b.IdleSince); d != 0 {
			return d
		}
		return strings.Compare(a.Machine.ID, b.Machine.ID)
	})
	for _, victim := range candidates {
		if victim.Retirement == nil {
			continue
		}
		fenced, err := victim.Retirement.Retire(ctx)
		if err != nil {
			return r.failed(st, ActionDestroyFailed, len(nodes), "retire", err)
		}
		if !fenced {
			// The snapshot said idle; the control plane says a claim landed
			// since. The node is not a victim any more. Try the next one.
			continue
		}
		err = driver.Destroy(ctx, victim.Machine.ID)
		if err != nil && !errors.Is(err, provision.ErrNotFound) {
			if !provision.IsDestroyNotApplied(err) {
				return r.failed(st, ActionDestroyFailed, len(nodes), "idle", err)
			}
			if releaseErr := victim.Retirement.Release(ctx); releaseErr != nil {
				err = errors.Join(err, releaseErr)
			}
			return r.failed(st, ActionDestroyFailed, len(nodes), "idle", err)
		}
		st.failures, st.retryAt = 0, time.Time{}
		return []Action{{Kind: ActionDestroyed, Machine: provision.CloneMachine(victim.Machine), From: len(nodes), To: len(nodes) - 1, Reason: "idle"}}, nil
	}
	st.failures, st.retryAt = 0, time.Time{}
	return nil, nil
}

func (r *Reconciler) failed(st *state, kind string, count int, reason string, err error) ([]Action, error) {
	st.failures++
	delay := r.opts.MinBackoff
	for i := 1; i < st.failures && delay < r.opts.MaxBackoff; i++ {
		delay = min(delay*2, r.opts.MaxBackoff)
	}
	st.retryAt = r.opts.Now().Add(delay)
	action := Action{Kind: kind, From: count, To: count, Reason: reason, Error: err.Error(), RetryAt: st.retryAt}
	return []Action{action}, err
}

func (r *Reconciler) poolState(key string) *state {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st := r.states[key]; st != nil {
		return st
	}
	st := &state{pending: map[string]time.Time{}}
	r.states[key] = st
	return st
}

func ownedNodes(spec Spec, nodes []Node) ([]Node, error) {
	out := make([]Node, 0, len(nodes))
	seen := map[string]struct{}{}
	for _, node := range nodes {
		machine := node.Machine
		if machine.Provider != spec.Vendor || machine.Tenant != spec.Tenant || machine.Labels[provision.PoolLabel] != spec.Name {
			continue
		}
		if machine.ID == "" {
			return nil, errors.New("pool: provider inventory contains an empty machine id")
		}
		if _, duplicate := seen[machine.ID]; duplicate {
			return nil, fmt.Errorf("pool: provider inventory repeats machine %q", machine.ID)
		}
		seen[machine.ID] = struct{}{}
		node.Machine = provision.CloneMachine(machine)
		out = append(out, node)
	}
	return out, nil
}

func (s Spec) key() string { return s.Tenant + "\x00" + s.Name }

// Inventory reads provider-owned machines for exactly one tenant and pool.
// The provider driver applies the boundary and this method verifies it again
// before control uses the result for irreversible scale-down decisions.
func (r *Reconciler) Inventory(ctx context.Context, spec Spec) ([]provision.Machine, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	driver := r.drivers[spec.Vendor]
	if driver == nil {
		return nil, fmt.Errorf("pool: vendor %q is not configured", spec.Vendor)
	}
	machines, err := driver.List(ctx, provision.ListOptions{Pool: spec.Name, Tenant: spec.Tenant})
	if err != nil {
		return nil, err
	}
	out := make([]provision.Machine, 0, len(machines))
	for _, machine := range machines {
		if machine.Provider != spec.Vendor || machine.Tenant != spec.Tenant || machine.Labels[provision.PoolLabel] != spec.Name {
			return nil, errors.New("pool: provider inventory escaped its tenant or pool boundary")
		}
		if machine.ID == "" {
			return nil, errors.New("pool: provider inventory contains an empty machine id")
		}
		out = append(out, provision.CloneMachine(machine))
	}
	provision.SortMachines(out)
	return out, nil
}

func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for key, value := range in {
		out[key] = value
	}
	return out
}
