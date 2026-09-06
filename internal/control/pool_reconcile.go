package control

import (
	"context"
	"sort"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	nodepool "remount.dev/remount/internal/pool"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/provision"
)

// PoolBootstrap is the non-secret, deployment-wide node bootstrap template.
// Enrollment credentials and node identities are minted per machine.
type PoolBootstrap struct {
	ServerURL string `json:"server_url"`
	BinaryURL string `json:"binary_url"`
	DataDir   string `json:"data_dir"`
}

// PoolReconciler is the irreversible provider boundary used by control.
// Inventory is observed before every decision so a crash after provider create
// converges without duplicating capacity.
type PoolReconciler interface {
	Inventory(context.Context, nodepool.Spec) ([]provision.Machine, error)
	Reconcile(context.Context, nodepool.Spec, []nodepool.Node, int) ([]nodepool.Action, error)
	Forget(nodepool.Spec)
}

type poolWork struct {
	key    string
	pool   *proto.Pool
	source *proto.Pool
	spec   nodepool.Spec
	demand int
}

func (c *Control) poolSpec(in *proto.Pool) nodepool.Spec {
	return nodepool.Spec{
		Name: in.Spec.Name, Tenant: in.Tenant, Vendor: in.Spec.Vendor,
		Min: in.Spec.Min, Max: in.Spec.Max, Labels: cloneMap(in.Spec.Labels), Backend: in.Spec.Backend,
		IdleScaleDown: time.Duration(in.Spec.IdleScaleDownMilli) * time.Millisecond,
		Region:        in.Spec.Region, Size: in.Spec.Size,
		Bootstrap: provision.Bootstrap{ServerURL: c.opts.PoolBootstrap.ServerURL,
			BinaryURL: c.opts.PoolBootstrap.BinaryURL, DataDir: c.opts.PoolBootstrap.DataDir},
	}
}

// reconcilePoolsAsync snapshots scheduling demand and starts at most one
// provider operation per pool. Provider calls never hold control.mu or delay
// lease expiry and renew processing.
func (c *Control) reconcilePoolsAsync() {
	if c.opts.PoolReconciler == nil {
		return
	}
	c.mu.Lock()
	keys := make([]string, 0, len(c.pools))
	for key := range c.pools {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	demand := c.poolDemandLocked(keys)
	work := make([]poolWork, 0, len(keys))
	now := c.opts.Now()
	for _, key := range keys {
		if c.poolBusy[key] {
			continue
		}
		if retryAt := c.poolInventoryRetryAt[key]; retryAt.After(now) {
			continue
		}
		source := c.pools[key]
		p := clonePool(source)
		if p == nil {
			continue
		}
		c.poolBusy[key] = true
		work = append(work, poolWork{key: key, pool: p, source: source, spec: c.poolSpec(p), demand: demand[key]})
	}
	c.poolWG.Add(len(work))
	c.mu.Unlock()
	for _, item := range work {
		go c.reconcilePool(item)
	}
}

// poolDemandLocked assigns each currently unschedulable pending workspace to
// the first matching pool. A single claim cannot fan out into several VMs.
func (c *Control) poolDemandLocked(keys []string) map[string]int {
	out := make(map[string]int, len(keys))
	for _, ws := range c.workspaces {
		if ws.State != proto.WSPending || ws.Spec.Placement.Node != "" {
			continue
		}
		eligible := false
		for _, node := range c.nodes {
			if c.eligibleLocked(ws, node) {
				eligible = true
				break
			}
		}
		if eligible {
			continue
		}
		for _, key := range keys {
			pool := c.pools[key]
			if pool != nil && poolMatchesWorkspace(pool, ws) {
				out[key]++
				break
			}
		}
	}
	return out
}

func poolMatchesWorkspace(pool *proto.Pool, ws *proto.Workspace) bool {
	if pool.Tenant != ws.Tenant || (ws.Spec.Requires.Backend != "" && ws.Spec.Requires.Backend != pool.Spec.Backend) {
		return false
	}
	for key, value := range ws.Spec.Placement.Allow {
		if key == provision.PoolLabel {
			if value != pool.Spec.Name {
				return false
			}
			continue
		}
		if pool.Spec.Labels[key] != value {
			return false
		}
	}
	return true
}

func (c *Control) reconcilePool(work poolWork) {
	defer c.poolWG.Done()
	defer func() {
		c.mu.Lock()
		delete(c.poolBusy, work.key)
		c.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(c.requestCtx, 3*time.Minute)
	defer cancel()
	machines, err := c.opts.PoolReconciler.Inventory(ctx, work.spec)
	if err != nil {
		c.commitPoolFailure(work, "inventory", err)
		return
	}
	c.mu.Lock()
	delete(c.poolInventoryFailures, work.key)
	delete(c.poolInventoryRetryAt, work.key)
	c.mu.Unlock()
	nodes := c.enrichPoolInventory(work, machines)
	actions, reconcileErr := c.opts.PoolReconciler.Reconcile(ctx, work.spec, nodes, work.demand)
	c.commitPoolResult(work, len(machines), actions, reconcileErr)
}

func (c *Control) enrichPoolInventory(work poolWork, machines []provision.Machine) []nodepool.Node {
	now := c.opts.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]nodepool.Node, 0, len(machines))
	visible := make(map[string]struct{}, len(machines))
	for _, machine := range machines {
		visible[machine.ID] = struct{}{}
		node := nodepool.Node{Machine: provision.CloneMachine(machine), Workspaces: -1}
		nodeID := machine.Labels[provision.NodeLabel]
		if c.poolNodeIdentityLocked(work, nodeID) {
			node.Workspaces = 0
			for _, ws := range c.workspaces {
				if ws.Node == nodeID && held(ws.State) {
					node.Workspaces++
				}
			}
			key := poolMachine{pool: work.key, machine: machine.ID}
			if node.Workspaces == 0 {
				if c.poolIdle[key].IsZero() {
					c.poolIdle[key] = now
				}
				node.IdleSince = c.poolIdle[key]
				node.Retirement = &poolRetireFence{c: c, work: work, node: nodeID, machine: machine.ID}
			} else {
				delete(c.poolIdle, key)
			}
		}
		out = append(out, node)
	}
	c.prunePoolRetirementsLocked(work, visible)
	// Prune only this pool's own entries. The map is control-wide, and
	// reconcilePoolsAsync starts every pool on the same tick, so a prune keyed
	// on machine id alone made pool A erase pool B's idle clocks: no machine in
	// a multi-pool fleet ever survived from "marked idle" to "old enough to
	// scale down", and idle provider inventory ran indefinitely. Retained
	// inventory costs money, so this is a billing bug, not a tidiness one.
	for key := range c.poolIdle {
		if key.pool != work.key {
			continue
		}
		if _, ok := visible[key.machine]; !ok {
			delete(c.poolIdle, key)
		}
	}
	return out
}

// poolMachine scopes an idle clock to the pool that observed it. Two pools may
// legitimately see the same provider machine id, and neither may speak for the
// other's inventory.
type poolMachine struct {
	pool    string
	machine string
}

// poolRetirement is one durable scale-down fence: the pool that selected the
// node, the exact provider machine it intends to destroy, and the node id
// whose claims are refused meanwhile. It is keyed by node in c.poolRetiring
// and in the pool_retirements table.
type poolRetirement struct {
	Pool    string
	Machine string
	Node    string
}

// poolRetireFence is the nodepool.Retirement control hands out with every
// idle node in an inventory snapshot. Retire and Release take c.mu and touch
// only control state; the provider is never called under that lock.
type poolRetireFence struct {
	c       *Control
	work    poolWork
	node    string
	machine string
}

func (f *poolRetireFence) payload() map[string]any {
	return map[string]any{"pool": f.work.pool.Spec.Name, "machine": f.machine, "node": f.node}
}

// Retire re-checks the node against current assignment state and, only if it
// is still this pool's online, assignment-free node, durably fences it from
// new claims. False means the snapshot went stale: a claim landed, the node
// changed identity, or the pool moved on. Re-fencing a node this pool already
// fenced for the same machine is a no-op that returns true.
func (f *poolRetireFence) Retire(context.Context) (bool, error) {
	c := f.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pools[f.work.key] != f.work.source {
		return false, nil
	}
	if existing, ok := c.poolRetiring[f.node]; ok {
		return existing.Pool == f.work.key && existing.Machine == f.machine, nil
	}
	if !c.poolNodeIdentityLocked(f.work, f.node) {
		return false, nil
	}
	for _, ws := range c.workspaces {
		if ws.Node == f.node && held(ws.State) {
			return false, nil
		}
	}
	fence := poolRetirement{Pool: f.work.key, Machine: f.machine, Node: f.node}
	if err := c.transact(func(tx *eventlog.Tx) error {
		_, err := tx.Exec(`INSERT INTO pool_retirements(node, tenant, pool, machine, created_at) VALUES(?,?,?,?,?)`,
			f.node, f.work.pool.Tenant, f.work.pool.Spec.Name, f.machine, c.opts.Now().UnixMilli())
		return err
	}, []*proto.Event{c.poolEvent(proto.EvPoolRetiring, f.work.pool, "", f.payload())}); err != nil {
		return false, err
	}
	c.poolRetiring[f.node] = fence
	return true, nil
}

// Release lifts the fence after a definite provider failure, so the node
// returns to service. A fence held by another pool or for another machine is
// not this caller's to lift.
func (f *poolRetireFence) Release(context.Context) error {
	c := f.c
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, ok := c.poolRetiring[f.node]
	if !ok || existing.Pool != f.work.key || existing.Machine != f.machine {
		return nil
	}
	if err := c.transact(func(tx *eventlog.Tx) error {
		_, err := tx.Exec(`DELETE FROM pool_retirements WHERE node=?`, f.node)
		return err
	}, []*proto.Event{c.poolEvent(proto.EvPoolRetireAborted, f.work.pool, "", f.payload())}); err != nil {
		return err
	}
	delete(c.poolRetiring, f.node)
	return nil
}

// poolNodeIdentityLocked reports whether node is an online control node whose
// enrollment labels place it in exactly this pool and tenant.
func (c *Control) poolNodeIdentityLocked(work poolWork, node string) bool {
	state := c.nodes[node]
	return node != "" && state != nil && state.Status.Online &&
		state.Status.Labels[provision.PoolLabel] == work.pool.Spec.Name &&
		state.Status.Labels["tenant"] == work.pool.Tenant
}

// prunePoolRetirementsLocked releases this pool's fences whose machine the
// provider no longer lists: the destroy took effect, or the machine vanished
// on its own. Until inventory says so, a fence outlives even a successful
// destroy, because the node may still be connected. A fence whose row cannot
// be deleted stays in force; refusing claims is the safe failure.
func (c *Control) prunePoolRetirementsLocked(work poolWork, visible map[string]struct{}) {
	for node, fence := range c.poolRetiring {
		if fence.Pool != work.key {
			continue
		}
		if _, ok := visible[fence.Machine]; ok {
			continue
		}
		payload := map[string]any{"pool": work.pool.Spec.Name, "machine": fence.Machine, "node": node}
		if err := c.transact(func(tx *eventlog.Tx) error {
			_, err := tx.Exec(`DELETE FROM pool_retirements WHERE node=?`, node)
			return err
		}, []*proto.Event{c.poolEvent(proto.EvPoolRetired, work.pool, "", payload)}); err != nil {
			c.logger.Error("release pool retirement", "pool", work.pool.Spec.Name, "node", node, "err", err)
			continue
		}
		delete(c.poolRetiring, node)
	}
}

// poolInventoryBackoff bounds how often a failing inventory call is retried.
// Without it a broken helper or a provider outage was polled, and recorded as
// a pool.provision_failed event, on every reconcile tick.
const (
	poolInventoryBackoffMin = 2 * time.Second
	poolInventoryBackoffMax = 2 * time.Minute
)

func (c *Control) commitPoolFailure(work poolWork, reason string, cause error) {
	var retryAt time.Time
	if reason == "inventory" {
		c.mu.Lock()
		c.poolInventoryFailures[work.key]++
		delay := poolInventoryBackoffMin
		for i := 1; i < c.poolInventoryFailures[work.key] && delay < poolInventoryBackoffMax; i++ {
			delay = min(delay*2, poolInventoryBackoffMax)
		}
		retryAt = c.opts.Now().Add(delay)
		c.poolInventoryRetryAt[work.key] = retryAt
		c.mu.Unlock()
	}
	c.commitPoolResult(work, work.pool.Current, []nodepool.Action{{Kind: nodepool.ActionCreateFailed,
		From: work.pool.Current, To: work.pool.Current, Reason: reason, Error: cause.Error(), RetryAt: retryAt}}, cause)
}

func (c *Control) commitPoolResult(work poolWork, observed int, actions []nodepool.Action, reconcileErr error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.pools[work.key]
	if current == nil || current != work.source {
		return
	}
	next := clonePool(current)
	events := make([]*proto.Event, 0, len(actions)+1)
	for _, action := range actions {
		switch action.Kind {
		case nodepool.ActionCreated, nodepool.ActionDestroyed:
			next.Current = action.To
			events = append(events, c.poolEvent(proto.EvPoolScaled, next, "", map[string]any{
				"pool": next.Spec.Name, "from": action.From, "to": action.To, "reason": action.Reason,
				"machine": action.Machine.ID,
			}))
			metrics.PoolScaleActions.Inc()
		case nodepool.ActionCreateFailed, nodepool.ActionDestroyFailed:
			events = append(events, c.poolEvent(proto.EvPoolFailed, next, "", map[string]any{
				"pool": next.Spec.Name, "reason": action.Reason, "retry_at": action.RetryAt.UnixMilli(), "error": action.Error,
			}))
			metrics.PoolProvisionFailures.Inc()
		}
	}
	if len(actions) == 0 && next.Current != observed {
		from := next.Current
		next.Current = observed
		events = append(events, c.poolEvent(proto.EvPoolScaled, next, "", map[string]any{
			"pool": next.Spec.Name, "from": from, "to": observed, "reason": "inventory_reconciled",
		}))
	}
	if len(events) == 0 {
		if reconcileErr != nil {
			c.logger.Error("reconcile pool", "pool", work.pool.Spec.Name, "err", reconcileErr)
		}
		return
	}
	next.UpdatedAt = c.opts.Now().UnixMilli()
	if err := c.transact(func(tx *eventlog.Tx) error {
		_, err := tx.Exec(`UPDATE pools SET data=? WHERE tenant=? AND name=?`, proto.MustMarshal(next), next.Tenant, next.Spec.Name)
		return err
	}, events); err != nil {
		c.logger.Error("persist pool reconcile", "pool", next.Spec.Name, "err", err)
		return
	}
	c.pools[work.key] = next
	metrics.PoolMachines.Set(c.poolMachineCountLocked())
}

func (c *Control) poolMachineCountLocked() int64 {
	var total int64
	for _, pool := range c.pools {
		total += int64(pool.Current)
	}
	return total
}
