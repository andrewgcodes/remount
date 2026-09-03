package control

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

func TestLeaseExpiryFailsClosedWhenGenerationIsExhausted(t *testing.T) {
	now := time.Unix(2_200_000_000, 0)
	f := newControlFixture(t, "", func(options *Options) {
		options.Now = func() time.Time { return now }
	})
	workspace := &proto.Workspace{
		ID: "ws_exhausted", State: proto.WSClaimed, Node: "n_owner",
		Generation: uint64(math.MaxInt64), LeaseUntil: now.Add(-time.Second).UnixMilli(),
		Tenant: "tenant", Owner: "owner", UpdatedAt: now.Add(-time.Minute).UnixMilli(),
	}
	f.c.mu.Lock()
	if err := f.c.persistWS(workspace); err != nil {
		f.c.mu.Unlock()
		t.Fatal(err)
	}
	f.c.workspaces[workspace.ID] = workspace
	f.c.mu.Unlock()

	f.c.Tick(context.Background())
	got := f.c.snapshotWS(workspace.ID)
	if got.State != proto.WSFailed || got.Generation != uint64(math.MaxInt64) || got.Node != "n_owner" || got.LeaseUntil != 0 {
		t.Fatalf("exhausted lease transition = %#v", got)
	}
	events, err := f.log.Read(context.Background(), 1, workspace.ID, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != proto.EvWSLeaseExpired {
		t.Fatalf("lease events = %#v", events)
	}
}

func TestWorkspaceGenerationNeverWrapsOrExceedsPersistenceRange(t *testing.T) {
	got, err := nextWorkspaceGeneration(uint64(math.MaxInt64 - 1))
	if err != nil || got != uint64(math.MaxInt64) {
		t.Fatalf("last generation = (%d, %v)", got, err)
	}
	if _, err := nextWorkspaceGeneration(uint64(math.MaxInt64)); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("generation exhaustion = %v", err)
	}
	workspace := &proto.Workspace{Generation: 1, AuthzRevision: math.MaxUint64}
	if err := canAdvanceWorkspaceAuthority(workspace); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("authorization exhaustion = %v", err)
	}
}

func TestWorkspaceStateMachineExhaustive(t *testing.T) {
	states := []string{
		proto.WSPending, proto.WSClaiming, proto.WSClaimed, proto.WSQuiescing,
		proto.WSCheckpointing, proto.WSPaused, proto.WSReleased,
		proto.WSDestroying, proto.WSDestroyed, proto.WSFailed,
	}
	for operation, rule := range lifecycleRules {
		for _, from := range states {
			for _, to := range states {
				workspace := &proto.Workspace{ID: "ws_state", State: from, Generation: 7, Node: "n_owner"}
				_, err := transitionWorkspace(workspace, lifecycleTransition{
					operation: operation, actor: rule.actor, to: to,
					expectGeneration: true, generation: 7,
					expectNode: true, node: "n_owner",
				})
				_, allowed := rule.pairs[statePair{from, to}]
				if allowed != (err == nil) {
					t.Fatalf("%s %s -> %s: allowed=%t err=%v", operation, from, to, allowed, err)
				}
			}
		}
	}
}

func TestWorkspaceStateMachineRejectsWrongAuthority(t *testing.T) {
	workspace := &proto.Workspace{ID: "ws_state", State: proto.WSClaiming, Generation: 9, Node: "n_owner"}
	base := lifecycleTransition{
		operation: transitionReady, actor: actorNode, to: proto.WSClaimed,
		expectGeneration: true, generation: 9, expectNode: true, node: "n_owner",
	}
	for name, mutate := range map[string]func(*lifecycleTransition){
		"actor":      func(request *lifecycleTransition) { request.actor = actorControl },
		"generation": func(request *lifecycleTransition) { request.generation-- },
		"node":       func(request *lifecycleTransition) { request.node = "n_stale" },
	} {
		t.Run(name, func(t *testing.T) {
			request := base
			mutate(&request)
			_, err := transitionWorkspace(workspace, request)
			var protocolError *proto.Error
			if err == nil || !errors.As(err, &protocolError) {
				t.Fatalf("transition error = %v", err)
			}
		})
	}
}

func TestWorkspaceStateMachineReorderedLifecycleProperty(t *testing.T) {
	for seed := int64(0); seed < 1_000; seed++ {
		random := rand.New(rand.NewSource(seed))
		workspace := proto.Workspace{ID: "ws_state", State: proto.WSPending}
		owner := ""
		for step := 0; step < 100; step++ {
			node := []string{"n_one", "n_two"}[random.Intn(2)]
			generation := uint64(random.Intn(20))
			request := lifecycleTransition{
				operation: transitionClaim, actor: actorNode, to: proto.WSClaiming,
				expectGeneration: true, generation: generation,
			}
			if workspace.State == proto.WSClaiming || workspace.State == proto.WSClaimed {
				request.operation = transitionReady
				request.to = proto.WSClaimed
				request.expectNode = true
				request.node = node
			}
			next, err := transitionWorkspace(&workspace, request)
			if err != nil {
				continue
			}
			if request.operation == transitionClaim {
				next.Generation++
				next.Node = node
				owner = node
			}
			workspace = next
			if workspace.State == proto.WSClaimed && (workspace.Node == "" || workspace.Node != owner) {
				t.Fatalf("seed %d step %d made stale node serviceable: %+v owner=%q", seed, step, workspace, owner)
			}
		}
	}
}
