package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/control/replicate"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
)

// ReplicationOptions wires an already-acquired writer lease into one server.
// A standby restores its database before calling New, then supplies Recovery.
type ReplicationOptions struct {
	Store       replicate.Store
	Coordinator *replicate.Coordinator
	Options     replicate.Options
	Recovery    *replicate.Recovery
}

type controlReplication struct {
	coordinator *replicate.Coordinator
	shipper     *replicate.Shipper
	log         *eventlog.Log
	shipEvery   time.Duration
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	keepalive   sync.Once
	shipping    sync.Once
	mu          sync.RWMutex
	err         error
	last        replicate.Manifest
	recovered   bool
	control     *control.Control
}

func newControlReplication(db *sql.DB, databasePath, tempDir string, log *eventlog.Log, controller *control.Control, options *ReplicationOptions) (*controlReplication, error) {
	if options == nil {
		return nil, nil
	}
	if options.Store == nil || options.Coordinator == nil || databasePath == "" || databasePath == ":memory:" {
		return nil, errors.New("server: replication requires a store, acquired coordinator, and file-backed database")
	}
	if _, err := options.Coordinator.CanDecide(); err != nil {
		return nil, fmt.Errorf("server: replication writer lease is not authoritative: %w", err)
	}
	maxSnapshot := options.Options.MaxSnapshotBytes
	if maxSnapshot == 0 {
		maxSnapshot = 1 << 30
	}
	maxWAL := options.Options.MaxWALBytes
	if maxWAL == 0 {
		maxWAL = 512 << 20
	}
	source, err := replicate.NewSQLiteSource(db, databasePath, tempDir, maxSnapshot, maxWAL, time.Now)
	if err != nil {
		return nil, err
	}
	shipper, err := replicate.NewShipper(options.Store, source, options.Coordinator, options.Options)
	if err != nil {
		return nil, err
	}
	interval := options.Options.ShipInterval
	if interval == 0 {
		interval = time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &controlReplication{coordinator: options.Coordinator, shipper: shipper, log: log, shipEvery: interval, ctx: ctx, cancel: cancel, recovered: options.Recovery == nil, control: controller}, nil
}

func (r *controlReplication) initialShip(ctx context.Context) error {
	seq, err := r.log.Last(ctx)
	if err != nil {
		return err
	}
	manifest, err := r.shipper.Ship(ctx, seq, true)
	if err != nil {
		return err
	}
	r.recordManifest(manifest)
	return nil
}

func (r *controlReplication) startKeepAlive() {
	r.keepalive.Do(func() {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			if err := r.coordinator.KeepAlive(r.ctx); err != nil && !errors.Is(err, context.Canceled) {
				r.fail(err)
			}
		}()
	})
}

func (r *controlReplication) startShipping() {
	r.shipping.Do(func() {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			ticker := time.NewTicker(r.shipEvery)
			defer ticker.Stop()
			for {
				select {
				case <-r.ctx.Done():
					return
				case <-ticker.C:
					seq, err := r.log.Last(r.ctx)
					if err == nil {
						var manifest replicate.Manifest
						manifest, err = r.shipper.Ship(r.ctx, seq, false)
						if err == nil {
							r.recordManifest(manifest)
						}
					}
					if err != nil && !errors.Is(err, context.Canceled) {
						metrics.ControllerShipFailures.Inc()
						if errors.Is(err, replicate.ErrFenced) {
							r.fail(err)
							return
						}
					}
				}
			}
		}()
	})
}

func (r *controlReplication) recordManifest(manifest replicate.Manifest) {
	r.mu.Lock()
	r.last = manifest
	r.mu.Unlock()
	r.control.RecordControllerReplication(manifest.CapturedAt)
	metrics.ControllerEpoch.Set(int64(manifest.Epoch))
	lag := time.Since(manifest.CapturedAt)
	if lag < 0 {
		lag = 0
	}
	metrics.ControllerReplicationLagMS.Set(lag.Milliseconds())
}

func (r *controlReplication) fail(err error) {
	r.mu.Lock()
	if r.err == nil {
		r.err = err
	}
	r.mu.Unlock()
	r.cancel()
}

func (r *controlReplication) ready() error {
	r.mu.RLock()
	err := r.err
	recovered := r.recovered
	r.mu.RUnlock()
	if err != nil {
		return err
	}
	if !recovered {
		return errors.New("controller recovery has not been replicated")
	}
	_, err = r.coordinator.CanDecide()
	return err
}

func (r *controlReplication) markRecovered() {
	r.mu.Lock()
	r.recovered = true
	r.mu.Unlock()
}

func (r *controlReplication) close() {
	if r == nil {
		return
	}
	r.cancel()
	r.wg.Wait()
}
