package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	artifacts3 "remount.dev/remount/internal/artifact/s3"
	"remount.dev/remount/internal/control/replicate"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/server"
)

func prepareControllerReplication(ctx context.Context, dataDir, blob string, standby bool, leaseTTL, shipInterval, snapshotInterval time.Duration) (*server.ReplicationOptions, error) {
	if blob == "" {
		return nil, errors.New("control replication requires --blob s3://bucket/prefix")
	}
	cfg, err := s3ArtifactConfig(blob, 1<<30)
	if err != nil {
		return nil, err
	}
	store, err := artifacts3.New(cfg)
	if err != nil {
		return nil, err
	}
	options := replicate.Options{
		Prefix: "control/replication", LeaseTTL: leaseTTL, RenewInterval: leaseTTL / 3,
		ShipInterval: shipInterval, SnapshotInterval: snapshotInterval,
	}
	coordinator, err := replicate.NewCoordinator(store, ids.New("ctrl"), options)
	if err != nil {
		return nil, err
	}
	result := &server.ReplicationOptions{Store: store, Coordinator: coordinator, Options: options}
	if !standby {
		if _, _, err := coordinator.Acquire(ctx); err != nil {
			return nil, fmt.Errorf("acquire active controller lease: %w", err)
		}
		return result, nil
	}
	restorer, err := replicate.NewRestorer(store, coordinator, options)
	if err != nil {
		return nil, err
	}
	destination := filepath.Join(dataDir, "control.db")
	for {
		recovery, promoteErr := restorer.Promote(ctx, destination)
		if promoteErr == nil {
			result.Recovery = &recovery
			return result, nil
		}
		if !errors.Is(promoteErr, replicate.ErrLeaseHeld) {
			return nil, fmt.Errorf("promote standby controller: %w", promoteErr)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
