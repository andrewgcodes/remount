package control

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"remount.dev/remount/internal/proto"
)

// ControllerAuthority is the local decision gate backed by the external
// conditional writer lease. CanDecide must be non-blocking; losing the lease
// makes every later control transaction fail closed.
type ControllerAuthority interface {
	CanDecide() (uint64, error)
}

// ControllerAuthorityReporter supplies lease diagnostics.
type ControllerAuthorityReporter interface {
	LeaseAge() time.Duration
	Fenced() bool
}

// RecoveryState describes the verified recovery point restored before a
// promoted controller opens its database.
type RecoveryState struct {
	PreviousEpoch    uint64
	RestoredEventSeq uint64
	LastReplicatedAt time.Time
	PromotedAt       time.Time
	LostWindow       time.Duration
}

func (c *Control) decisionEpoch() (uint64, error) {
	if c.controllerAuthority == nil {
		return c.controllerEpoch, nil
	}
	epoch, err := c.controllerAuthority.CanDecide()
	if err != nil {
		return 0, proto.Err(proto.CodeConflict, "controller writer lease is fenced: %v", err)
	}
	if epoch == 0 || epoch != c.controllerEpoch {
		return 0, proto.Err(proto.CodeConflict, "controller epoch changed from %d to %d", c.controllerEpoch, epoch)
	}
	return epoch, nil
}

// ControllerEpoch returns the immutable epoch acquired for this process. The
// relay stamps it on every control-originated post-hello frame.
func (c *Control) ControllerEpoch() uint64 { return c.controllerEpoch }

func (c *Control) validateFrameEpoch(f *proto.Frame) error {
	if f == nil {
		return proto.Err(proto.CodeBadRequest, "missing frame")
	}
	if f.ControllerEpoch != c.controllerEpoch {
		return proto.Err(proto.CodeConflict, "stale controller epoch %d; active epoch is %d", f.ControllerEpoch, c.controllerEpoch)
	}
	return nil
}

func (c *Control) peerRequiresControllerEpoch(peer string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if node := c.nodes[peer]; node != nil {
		return proto.HasCapability(node.Status.Protocol, proto.CapabilityControllerEpoch)
	}
	if hello := c.clients[peer]; hello != nil {
		return proto.HasCapability(hello.Caps, proto.CapabilityControllerEpoch)
	}
	return false
}

func (c *Control) stampControllerEpoch(events []*proto.Event) {
	for _, event := range events {
		if event != nil {
			event.ControllerEpoch = c.controllerEpoch
		}
	}
}

func (c *Control) validatePersistedControllerEpoch() error {
	var stored uint64
	err := c.db.QueryRow(`SELECT epoch FROM controller_state WHERE id=1`).Scan(&stored)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if stored > c.controllerEpoch {
		return fmt.Errorf("control: persisted controller epoch %d is newer than acquired epoch %d", stored, c.controllerEpoch)
	}
	if c.recovery != nil && stored >= c.controllerEpoch {
		return fmt.Errorf("control: promotion epoch %d did not advance restored epoch %d", c.controllerEpoch, stored)
	}
	return nil
}

// RecoveryPending reports whether a promoted controller is still reconciling
// node-authoritative state and must remain unready for client decisions.
func (c *Control) RecoveryPending() bool {
	c.recoveryMu.RLock()
	defer c.recoveryMu.RUnlock()
	return c.recoveryPending
}

func (c *Control) controllerRecoverySnapshot() (string, *RecoveryState, bool) {
	c.recoveryMu.RLock()
	defer c.recoveryMu.RUnlock()
	role := c.controllerRole
	var recovery *RecoveryState
	if c.recovery != nil {
		copyRecovery := *c.recovery
		recovery = &copyRecovery
	}
	return role, recovery, c.recoveryPending
}

// RecordControllerReplication publishes the last successfully committed
// recovery point to diagnostics. It is called only after current.json advances.
func (c *Control) RecordControllerReplication(at time.Time) {
	c.recoveryMu.Lock()
	c.lastReplicatedAt = at
	c.recoveryMu.Unlock()
}

func (c *Control) controllerLastReplicatedAt() time.Time {
	c.recoveryMu.RLock()
	defer c.recoveryMu.RUnlock()
	return c.lastReplicatedAt
}
