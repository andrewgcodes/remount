package control

import (
	"context"
	"fmt"
	"sort"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

func poolKey(tenant, name string) string { return tenant + "\x00" + name }

type mutationPoolResult struct {
	Name string `cbor:"name"`
}

func poolResource(pool *proto.Pool) Resource {
	return Resource{Kind: "pool", ID: pool.Spec.Name, Tenant: pool.Tenant, Owner: pool.Owner}
}

func clonePool(in *proto.Pool) *proto.Pool {
	if in == nil {
		return nil
	}
	out := *in
	out.Spec.Labels = cloneMap(in.Spec.Labels)
	return &out
}

func (c *Control) poolEvent(typ string, pool *proto.Pool, principal string, payload any) *proto.Event {
	e := c.newEvent(typ, pool.Spec.Name, principal, "", payload)
	e.Tenant = pool.Tenant
	return e
}

func (c *Control) loadPools() error {
	rows, err := c.db.Query(`SELECT data FROM pools`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return err
		}
		var pool proto.Pool
		if err := proto.Unmarshal(data, &pool); err != nil {
			return fmt.Errorf("decode pool: %w", err)
		}
		c.pools[poolKey(pool.Tenant, pool.Spec.Name)] = clonePool(&pool)
	}
	return rows.Err()
}

// loadPoolRetirements restores scale-down fences committed before a restart.
// A fenced node stays unclaimable until provider inventory shows whether its
// destroy took effect; the in-flight goroutine that held it died with the
// process, but the provider call it made may not have.
func (c *Control) loadPoolRetirements() error {
	rows, err := c.db.Query(`SELECT node, tenant, pool, machine FROM pool_retirements`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var node, tenant, pool, machine string
		if err := rows.Scan(&node, &tenant, &pool, &machine); err != nil {
			return err
		}
		c.poolRetiring[node] = poolRetirement{Pool: poolKey(tenant, pool), Machine: machine, Node: node}
	}
	return rows.Err()
}

func (c *Control) poolCreate(ctx context.Context, subject Subject, req *proto.PoolCreateReq) (*proto.Pool, error) {
	if err := proto.ValidatePoolSpec(req.Spec); err != nil {
		return nil, err
	}
	resource := Resource{Kind: "pool", ID: req.Spec.Name, Tenant: subject.Tenant, Owner: subject.ID}
	if err := c.check(ctx, subject, ActionAdmin, resource); err != nil {
		return nil, err
	}
	scope := subject.Tenant + "|" + subject.ID + "|pool.create"
	unlock := c.lockMutation(scope, req.IdempotencyKey)
	defer unlock()
	var prior mutationPoolResult
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpPoolCreate, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return c.poolGet(ctx, subject, prior.Name)
	}

	now := c.now().UnixMilli()
	pool := &proto.Pool{
		Spec: req.Spec, Tenant: subject.Tenant, Owner: subject.ID,
		CreatedAt: now, UpdatedAt: now,
	}
	pool.Spec.Labels = cloneMap(req.Spec.Labels)
	key := poolKey(pool.Tenant, pool.Spec.Name)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pools[key] != nil {
		return nil, proto.Err(proto.CodeConflict, "pool %q already exists", pool.Spec.Name)
	}
	err := c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT INTO pools(tenant, name, data) VALUES(?,?,?)`, pool.Tenant, pool.Spec.Name, proto.MustMarshal(pool)); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpPoolCreate, req, mutationPoolResult{Name: pool.Spec.Name})
	}, []*proto.Event{c.poolEvent(proto.EvPoolCreated, pool, subject.ID, map[string]any{
		"pool": pool.Spec.Name, "vendor": pool.Spec.Vendor, "min": pool.Spec.Min, "max": pool.Spec.Max,
	})})
	if err != nil {
		return nil, err
	}
	c.pools[key] = clonePool(pool)
	return clonePool(pool), nil
}

func (c *Control) poolGet(ctx context.Context, subject Subject, name string) (*proto.Pool, error) {
	c.mu.Lock()
	pool := clonePool(c.pools[poolKey(subject.Tenant, name)])
	c.mu.Unlock()
	if pool == nil {
		return nil, proto.Err(proto.CodeNotFound, "pool %q is not defined", name)
	}
	if err := c.check(ctx, subject, ActionRead, poolResource(pool)); err != nil {
		return nil, err
	}
	return pool, nil
}

func (c *Control) poolList(ctx context.Context, subject Subject) (*proto.PoolListRes, error) {
	c.mu.Lock()
	pools := make([]*proto.Pool, 0, len(c.pools))
	for _, pool := range c.pools {
		pools = append(pools, clonePool(pool))
	}
	c.mu.Unlock()
	result := &proto.PoolListRes{Pools: []proto.Pool{}}
	for _, pool := range pools {
		if pool.Tenant != subject.Tenant {
			continue
		}
		if err := c.check(ctx, subject, ActionRead, poolResource(pool)); err == nil {
			result.Pools = append(result.Pools, *pool)
		}
	}
	sort.Slice(result.Pools, func(i, j int) bool { return result.Pools[i].Spec.Name < result.Pools[j].Spec.Name })
	return result, nil
}

func (c *Control) poolRemove(ctx context.Context, subject Subject, req *proto.PoolRemoveReq) error {
	if req.Name == "" {
		return proto.Err(proto.CodeBadRequest, "pool name is required")
	}
	scope := subject.Tenant + "|" + subject.ID + "|pool.remove"
	unlock := c.lockMutation(scope, req.IdempotencyKey)
	defer unlock()
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpPoolRemove, req, nil); err != nil {
		return err
	} else if hit {
		return nil
	}
	key := poolKey(subject.Tenant, req.Name)
	c.mu.Lock()
	pool := clonePool(c.pools[key])
	c.mu.Unlock()
	if pool == nil {
		return proto.Err(proto.CodeNotFound, "pool %q is not defined", req.Name)
	}
	if err := c.check(ctx, subject, ActionAdmin, poolResource(pool)); err != nil {
		return err
	}
	c.mu.Lock()
	current := c.pools[key]
	if current == nil || current.UpdatedAt != pool.UpdatedAt {
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "pool %q changed during removal", req.Name)
	}
	if c.poolBusy[key] {
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "pool %q is reconciling; retry removal", req.Name)
	}
	if current.Current != 0 {
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "pool %q still owns %d machines", req.Name, current.Current)
	}
	// A retirement fence outlives its destroy until inventory no longer lists
	// the machine. Removing the pool would remove the only reconciler that can
	// confirm that, so the fence must resolve first; the node stays unclaimable
	// meanwhile and the removal is retryable.
	for node, fence := range c.poolRetiring {
		if fence.Pool == key {
			c.mu.Unlock()
			return proto.Err(proto.CodeConflict, "pool %q is still retiring node %s; retry after its machine leaves provider inventory", req.Name, node)
		}
	}
	if err := c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`DELETE FROM pools WHERE tenant=? AND name=?`, pool.Tenant, pool.Spec.Name); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpPoolRemove, req, struct{}{})
	}, []*proto.Event{c.poolEvent(proto.EvPoolRemoved, pool, subject.ID, map[string]any{"pool": pool.Spec.Name})}); err != nil {
		c.mu.Unlock()
		return err
	}
	delete(c.pools, key)
	c.mu.Unlock()
	if c.opts.PoolReconciler != nil {
		c.opts.PoolReconciler.Forget(c.poolSpec(pool))
	}
	return nil
}
