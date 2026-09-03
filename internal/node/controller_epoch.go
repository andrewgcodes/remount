package node

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"remount.dev/remount/internal/proto"
)

func loadControllerEpoch(path string) (uint64, error) {
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var state struct {
		Epoch uint64 `cbor:"epoch"`
	}
	if err := proto.Unmarshal(payload, &state); err != nil || state.Epoch == 0 {
		return 0, fmt.Errorf("node: corrupt controller epoch fence")
	}
	return state.Epoch, nil
}

func persistControllerEpoch(path string, epoch uint64) error {
	if epoch == 0 {
		return errors.New("node: controller epoch must be nonzero")
	}
	payload, err := proto.Marshal(struct {
		Epoch uint64 `cbor:"epoch"`
	}{Epoch: epoch})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".controller-epoch-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err = tmp.Chmod(0o600); err == nil {
		var written int
		written, err = tmp.Write(payload)
		if err == nil && written != len(payload) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, path)
	}
	if err == nil {
		err = syncParentDir(filepath.Dir(path))
	}
	return err
}

func (n *Node) acceptControllerEpoch(epoch uint64) error {
	n.epochMu.Lock()
	defer n.epochMu.Unlock()
	if epoch == 0 {
		return proto.Err(proto.CodeConflict, "missing controller epoch")
	}
	if epoch < n.controllerEpoch {
		return proto.Err(proto.CodeConflict, "stale controller epoch %d; node has persisted %d", epoch, n.controllerEpoch)
	}
	if epoch == n.controllerEpoch {
		return nil
	}
	if err := persistControllerEpoch(n.epochPath, epoch); err != nil {
		return proto.Err(proto.CodeInternal, "persist controller epoch before accepting authority: %v", err)
	}
	n.controllerEpoch = epoch
	return nil
}

func (n *Node) currentControllerEpoch() uint64 {
	n.epochMu.Lock()
	defer n.epochMu.Unlock()
	return n.controllerEpoch
}
