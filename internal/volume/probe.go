package volume

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

// ProbeBindMount verifies that this host can establish and remove the exact
// read-only bind mount used for workspace volumes. A node must not advertise
// volume capability merely because it is running on Linux.
func ProbeBindMount(ctx context.Context, parent string) error {
	dir, err := os.MkdirTemp(parent, ".volume-probe-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	sourcePath, targetPath := filepath.Join(dir, "source"), filepath.Join(dir, "target")
	if err := os.Mkdir(sourcePath, 0o700); err != nil {
		return err
	}
	if err := os.Mkdir(targetPath, 0o700); err != nil {
		return err
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.Open(targetPath)
	if err != nil {
		return err
	}
	defer target.Close()
	expectedSource, err := source.Stat()
	if err != nil {
		return err
	}
	engine := BindMountEngine{}
	if err := engine.MountReadOnly(ctx, source, target); err != nil {
		return err
	}
	status, inspectErr := engine.Inspect(ctx, target)
	unmountErr := engine.Unmount(context.WithoutCancel(ctx), target)
	if inspectErr != nil || !status.Mounted || !status.ReadOnly || status.Source == nil || !os.SameFile(expectedSource, status.Source) {
		if inspectErr == nil {
			inspectErr = errors.New("volume: probe did not verify the exact read-only source")
		}
		return errors.Join(inspectErr, unmountErr)
	}
	return unmountErr
}
