package firecracker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sync"

	"remount.dev/remount/internal/netns"
)

// NetworkOptions configure the concrete netns/TAP adapter.
type NetworkOptions struct {
	Dir     string
	Manager *netns.Manager
	UID     int
	GID     int
}

// SystemNetworkProvider owns durable namespace locators and creates a TAP in
// each deny-first netns. Startup synchronously revokes stale boundaries before
// allowing a workspace to be adopted with a new broker generation.
type SystemNetworkProvider struct {
	opts NetworkOptions
	mu   sync.Mutex
	live map[string]*systemNetworkLease
}

type networkRecord struct {
	Workspace  string      `json:"workspace"`
	Generation uint64      `json:"generation,omitempty"`
	Tap        string      `json:"tap,omitempty"`
	Guest      netip.Addr  `json:"guest,omitempty"`
	State      netns.State `json:"state"`
}

// NewSystemNetworkProvider reconstructs and revokes crash-left boundaries.
func NewSystemNetworkProvider(ctx context.Context, opts NetworkOptions) (*SystemNetworkProvider, error) {
	if opts.Dir == "" || opts.Manager == nil || opts.UID <= 0 || opts.GID <= 0 {
		return nil, errors.New("firecracker: network state directory, manager and non-root TAP owner are required")
	}
	dir, err := filepath.Abs(opts.Dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	opts.Dir = dir
	p := &SystemNetworkProvider{opts: opts, live: make(map[string]*systemNetworkLease)}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			return nil, fmt.Errorf("firecracker: unrecognized network state %q", entry.Name())
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var record networkRecord
		if err := json.Unmarshal(data, &record); err != nil || validateWorkspaceID(record.Workspace) != nil {
			return nil, fmt.Errorf("firecracker: invalid network state %q", entry.Name())
		}
		network, err := opts.Manager.Adopt(ctx, record.State)
		if err != nil {
			return nil, fmt.Errorf("firecracker: validate stale network %s: %w", record.Workspace, err)
		}
		if err := network.Revoke(ctx); err != nil {
			return nil, fmt.Errorf("firecracker: revoke stale network %s: %w", record.Workspace, err)
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	}
	if err := syncDirectory(dir); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *SystemNetworkProvider) Probe(ctx context.Context) error { return p.opts.Manager.Probe(ctx) }

func (p *SystemNetworkProvider) Reserve(ctx context.Context, workspace string) (NetworkLease, error) {
	if err := validateWorkspaceID(workspace); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if _, exists := p.live[workspace]; exists {
		p.mu.Unlock()
		return nil, fmt.Errorf("firecracker: network for %s is already reserved", workspace)
	}
	p.live[workspace] = nil
	p.mu.Unlock()
	reserved := true
	defer func() {
		if reserved {
			p.release(workspace, nil)
		}
	}()
	// Address allocation must precede broker bind. Generation authority is
	// still enforced by Prepare and its durable record, not this locator name.
	network, err := p.opts.Manager.Open(ctx, workspace, 1)
	if err != nil {
		return nil, err
	}
	lease := &systemNetworkLease{provider: p, workspace: workspace, network: network, state: network.State()}
	if err := lease.persist(); err != nil {
		_ = network.Revoke(context.Background())
		return nil, err
	}
	p.mu.Lock()
	p.live[workspace] = lease
	p.mu.Unlock()
	reserved = false
	return lease, nil
}

func (p *SystemNetworkProvider) release(workspace string, expected *systemNetworkLease) {
	p.mu.Lock()
	if current, ok := p.live[workspace]; ok && (expected == nil || current == expected) {
		delete(p.live, workspace)
	}
	p.mu.Unlock()
}

type systemNetworkLease struct {
	provider   *SystemNetworkProvider
	workspace  string
	network    *netns.Network
	state      netns.State
	mu         sync.Mutex
	generation uint64
	tap        string
	guest      netip.Addr
	revoked    bool
}

func (l *systemNetworkLease) HostAddress() netip.Addr { return l.network.HostAddress() }
func (l *systemNetworkLease) GuestAddress() netip.Addr {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.guest
}
func (l *systemNetworkLease) NamespacePath() string { return l.network.NamespacePath() }

func (l *systemNetworkLease) Prepare(ctx context.Context, generation uint64) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.revoked || generation == 0 {
		return "", errors.New("firecracker: invalid or revoked network generation")
	}
	if l.generation != 0 {
		if l.generation == generation {
			return l.tap, nil
		}
		return "", errors.New("firecracker: network generation changed after prepare")
	}
	sum := sha256.Sum256([]byte(l.workspace))
	l.tap = fmt.Sprintf("rmt%x", sum[:5])
	guest, err := l.network.PrepareTap(ctx, l.tap, l.provider.opts.UID, l.provider.opts.GID)
	if err != nil {
		return "", err
	}
	l.generation, l.guest = generation, guest
	l.state = l.network.State()
	if err := l.persist(); err != nil {
		_ = l.network.Revoke(context.Background())
		l.revoked = true
		return "", err
	}
	return l.tap, nil
}

func (l *systemNetworkLease) Activate(ctx context.Context, broker netip.AddrPort) error {
	l.mu.Lock()
	prepared := l.generation != 0 && !l.revoked
	l.mu.Unlock()
	if !prepared {
		return errors.New("firecracker: network is not prepared")
	}
	return l.network.Apply(ctx, broker)
}

func (l *systemNetworkLease) Revoke(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.revoked {
		return nil
	}
	if err := l.network.Revoke(ctx); err != nil {
		return err
	}
	path := l.recordPath()
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := syncDirectory(l.provider.opts.Dir); err != nil {
		return err
	}
	l.revoked = true
	l.provider.release(l.workspace, l)
	return nil
}

func (l *systemNetworkLease) persist() error {
	record := networkRecord{Workspace: l.workspace, Generation: l.generation, Tap: l.tap, Guest: l.guest, State: l.state}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return atomicBytes(l.recordPath(), data, 0o600)
}

func (l *systemNetworkLease) recordPath() string {
	sum := sha256.Sum256([]byte(l.workspace))
	return filepath.Join(l.provider.opts.Dir, fmt.Sprintf("net-%x.json", sum[:12]))
}

var _ NetworkProvider = (*SystemNetworkProvider)(nil)
var _ NetworkLease = (*systemNetworkLease)(nil)
