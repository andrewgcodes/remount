// Package netns owns the Linux network boundary used by isolated workspace
// backends. Kernel changes are expressed through Kernel so the fail-closed
// ordering can be tested without privileges.
package netns

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net/netip"
	"sync"
	"time"
)

// Kernel is the privileged, command-free Linux networking surface.
// Implementations must not return from DeleteVeth until the host link is gone.
type Kernel interface {
	Probe(context.Context) error
	Validate(context.Context, string, Link) error
	CreateNamespace(context.Context, string) (string, error)
	CreateVeth(context.Context, string, string, string) error
	Configure(context.Context, string, Link) error
	InstallDenyAll(context.Context, string) error
	PermitBroker(context.Context, string, netip.AddrPort) error
	BringUp(context.Context, string, string) error
	DeleteVeth(context.Context, string) error
	CloseNamespace(string) error
}

// Link is the generation-specific point-to-point network assigned to a
// workspace. Host and Guest are the two usable addresses of its /30.
type Link struct {
	Host      netip.Prefix
	Guest     netip.Prefix
	HostName  string
	GuestName string
}

// State is the durable locator needed to re-adopt a retained namespace after
// a node restart. It contains addresses and names, never credentials.
type State struct {
	Slot      uint16 `json:"slot"`
	Namespace string `json:"namespace"`
	Link      Link   `json:"link"`
}

// Manager allocates non-overlapping link-local /30s and creates namespaces.
type Manager struct {
	kernel Kernel
	mu     sync.Mutex
	used   map[uint16]struct{}
}

// NewManager constructs a manager using kernel.
func NewManager(kernel Kernel) *Manager {
	return &Manager{kernel: kernel, used: make(map[uint16]struct{})}
}

// NewSystemManager constructs a manager backed by Linux netlink. On other
// hosts Probe and Open report that the isolation boundary is unavailable.
func NewSystemManager() *Manager { return NewManager(newSystemKernel()) }

// Probe verifies that the current host can create and program the boundary.
func (m *Manager) Probe(ctx context.Context) error { return m.kernel.Probe(ctx) }

// Open creates a namespace in deny-all state. The veth is left down until a
// generation-specific broker endpoint has been installed.
func (m *Manager) Open(ctx context.Context, workspace string, generation uint64) (_ *Network, err error) {
	if workspace == "" || generation == 0 {
		return nil, fmt.Errorf("netns: workspace and non-zero generation are required")
	}
	slot, link, err := m.allocate(workspace, generation)
	if err != nil {
		return nil, err
	}
	release := true
	defer func() {
		if release {
			m.release(slot)
		}
	}()

	name := fmt.Sprintf("rm-%x-%x", slot, generation&0xffff)
	nsPath, err := m.kernel.CreateNamespace(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("create network namespace: %w", err)
	}
	createdLink := false
	defer func() {
		if err == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if createdLink {
			err = errors.Join(err, m.kernel.DeleteVeth(cleanupCtx, link.HostName))
		}
		err = errors.Join(err, m.kernel.CloseNamespace(nsPath))
	}()
	if err = m.kernel.CreateVeth(ctx, nsPath, link.HostName, link.GuestName); err != nil {
		return nil, fmt.Errorf("create veth: %w", err)
	}
	createdLink = true
	if err = m.kernel.Configure(ctx, nsPath, link); err != nil {
		return nil, fmt.Errorf("configure veth: %w", err)
	}
	// The deny chain precedes both runsc startup and link activation. A partial
	// setup can therefore never create an unfiltered interval.
	if err = m.kernel.InstallDenyAll(ctx, nsPath); err != nil {
		return nil, fmt.Errorf("install deny-all policy: %w", err)
	}

	release = false
	return &Network{manager: m, kernel: m.kernel, slot: slot, namespace: nsPath, link: link}, nil
}

// Adopt reattaches to a retained boundary after validating that both its
// namespace and host link still exist. A live slot is never silently reused.
func (m *Manager) Adopt(ctx context.Context, state State) (*Network, error) {
	if state.Namespace == "" || !state.Link.Host.IsValid() || !state.Link.Guest.IsValid() || state.Link.HostName == "" {
		return nil, errors.New("netns: invalid retained state")
	}
	if expected := linkForSlot(state.Slot); state.Link != expected {
		return nil, fmt.Errorf("netns: retained link does not match slot %d", state.Slot)
	}
	m.mu.Lock()
	if _, exists := m.used[state.Slot]; exists {
		m.mu.Unlock()
		return nil, fmt.Errorf("netns: retained slot %d is already active", state.Slot)
	}
	m.used[state.Slot] = struct{}{}
	m.mu.Unlock()
	if err := m.kernel.Validate(ctx, state.Namespace, state.Link); err != nil {
		m.release(state.Slot)
		return nil, fmt.Errorf("validate retained network: %w", err)
	}
	return &Network{manager: m, kernel: m.kernel, slot: state.Slot, namespace: state.Namespace, link: state.Link, active: true}, nil
}

// Network is one live workspace network namespace.
type Network struct {
	manager   *Manager
	kernel    Kernel
	slot      uint16
	namespace string
	link      Link

	mu      sync.Mutex
	broker  netip.AddrPort
	active  bool
	revoked bool
}

// NamespacePath is a kernel namespace path suitable for an OCI network
// namespace entry.
func (n *Network) NamespacePath() string { return n.namespace }

// HostAddress is the address on which the workspace broker must listen.
func (n *Network) HostAddress() netip.Addr { return n.link.Host.Addr() }

// GuestAddress returns the sandbox side of the veth.
func (n *Network) GuestAddress() netip.Addr { return n.link.Guest.Addr() }

// State returns the non-secret locator needed for restart adoption.
func (n *Network) State() State {
	return State{Slot: n.slot, Namespace: n.namespace, Link: n.link}
}

// Apply permits exactly one TCP destination and then activates the link.
func (n *Network) Apply(ctx context.Context, broker netip.AddrPort) (err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.revoked {
		return errors.New("netns: network is revoked")
	}
	if !broker.IsValid() || !broker.Addr().Is4() || broker.Port() == 0 {
		return n.failClosed(ctx, fmt.Errorf("netns: invalid broker endpoint %q", broker))
	}
	if broker.Addr() != n.link.Host.Addr() {
		return n.failClosed(ctx, fmt.Errorf("netns: broker %s is outside generation link %s", broker.Addr(), n.link.Host))
	}
	if n.active {
		if broker == n.broker {
			return nil
		}
		return n.failClosed(ctx, fmt.Errorf("netns: broker endpoint changed from %s to %s", n.broker, broker))
	}
	if err = n.kernel.PermitBroker(ctx, n.namespace, broker); err != nil {
		return n.failClosed(ctx, fmt.Errorf("install broker permit: %w", err))
	}
	if err = n.kernel.BringUp(ctx, n.namespace, n.link.HostName); err != nil {
		return n.failClosed(ctx, fmt.Errorf("activate veth: %w", err))
	}
	n.broker = broker
	n.active = true
	return nil
}

// Revoke synchronously deletes the host veth. It is idempotent; an error leaves
// the namespace reserved so a later retry cannot collide with a live link.
func (n *Network) Revoke(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.revokeLocked(ctx)
}

func (n *Network) failClosed(ctx context.Context, cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	revokeErr := n.revokeLocked(cleanupCtx)
	return errors.Join(cause, revokeErr)
}

func (n *Network) revokeLocked(ctx context.Context) error {
	if n.revoked {
		return nil
	}
	if err := n.kernel.DeleteVeth(ctx, n.link.HostName); err != nil {
		return fmt.Errorf("delete veth: %w", err)
	}
	if err := n.kernel.CloseNamespace(n.namespace); err != nil {
		return fmt.Errorf("close namespace: %w", err)
	}
	n.revoked = true
	n.active = false
	n.manager.release(n.slot)
	return nil
}

func (m *Manager) allocate(workspace string, generation uint64) (uint16, Link, error) {
	h := fnv.New32a()
	_, _ = fmt.Fprintf(h, "%s\x00%d", workspace, generation)
	start := uint16(h.Sum32() % 16384)
	m.mu.Lock()
	defer m.mu.Unlock()
	for offset := uint16(0); offset < 16384; offset++ {
		slot := (start + offset) % 16384
		if _, exists := m.used[slot]; exists {
			continue
		}
		m.used[slot] = struct{}{}
		return slot, linkForSlot(slot), nil
	}
	return 0, Link{}, errors.New("netns: link-local /30 pool exhausted")
}

func linkForSlot(slot uint16) Link {
	third := byte(slot >> 6)
	fourth := byte((slot & 0x3f) << 2)
	host := netip.AddrFrom4([4]byte{169, 254, third, fourth + 1})
	guest := netip.AddrFrom4([4]byte{169, 254, third, fourth + 2})
	return Link{
		Host: netip.PrefixFrom(host, 30), Guest: netip.PrefixFrom(guest, 30),
		HostName: fmt.Sprintf("rmh%04x", slot), GuestName: fmt.Sprintf("rmg%04x", slot),
	}
}

func (m *Manager) release(slot uint16) {
	m.mu.Lock()
	delete(m.used, slot)
	m.mu.Unlock()
}
