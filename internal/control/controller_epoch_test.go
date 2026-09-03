package control

import (
	"errors"
	"sync"
	"testing"

	"remount.dev/remount/internal/proto"
)

type testControllerAuthority struct {
	mu    sync.Mutex
	epoch uint64
	err   error
}

func (a *testControllerAuthority) CanDecide() (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.epoch, a.err
}

func (a *testControllerAuthority) fence() {
	a.mu.Lock()
	a.err = errors.New("lease lost")
	a.mu.Unlock()
}

func TestControllerEpochCommitsWithEventAndFencesLaterMutation(t *testing.T) {
	authority := &testControllerAuthority{epoch: 41}
	fixture := newControlFixture(t, "", func(options *Options) { options.ControllerAuthority = authority })
	workspace := proto.Workspace{ID: "ws_epoch", Tenant: "local", Owner: "owner", Generation: 1, State: proto.WSPending}
	fixture.c.mu.Lock()
	err := fixture.c.persistWS(&workspace, fixture.c.newEvent(proto.EvWSCreated, workspace.ID, workspace.Owner, "", nil))
	fixture.c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	events, err := fixture.log.Read(t.Context(), 1, "", 10)
	if err != nil || len(events) != 1 || events[0].ControllerEpoch != 41 {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	var stored uint64
	if err := fixture.sq.DB().QueryRow(`SELECT epoch FROM controller_state WHERE id=1`).Scan(&stored); err != nil || stored != 41 {
		t.Fatalf("controller_state epoch=%d err=%v", stored, err)
	}
	authority.fence()
	forbidden := proto.Workspace{ID: "ws_after_fence", Tenant: "local", Owner: "owner", Generation: 1, State: proto.WSPending}
	fixture.c.mu.Lock()
	err = fixture.c.persistWS(&forbidden, fixture.c.newEvent(proto.EvWSCreated, forbidden.ID, forbidden.Owner, "", nil))
	fixture.c.mu.Unlock()
	if err == nil {
		t.Fatal("fenced controller committed a workspace")
	}
	var count int
	if queryErr := fixture.sq.DB().QueryRow(`SELECT count(*) FROM workspaces WHERE id=?`, forbidden.ID).Scan(&count); queryErr != nil || count != 0 {
		t.Fatalf("forbidden workspace rows=%d err=%v", count, queryErr)
	}
}
