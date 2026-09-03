package node

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"remount.dev/remount/internal/proto"
)

func TestControllerEpochFencePersistsBeforeAcceptance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controller-epoch.cbor")
	node := &Node{epochPath: path}
	if err := node.acceptControllerEpoch(9); err != nil {
		t.Fatal(err)
	}
	if got, err := loadControllerEpoch(path); err != nil || got != 9 {
		t.Fatalf("loadControllerEpoch=(%d,%v), want 9", got, err)
	}
	restarted := &Node{epochPath: path, controllerEpoch: 9}
	if err := restarted.acceptControllerEpoch(8); err == nil {
		t.Fatal("restarted node accepted a lower controller epoch")
	} else {
		var wire *proto.Error
		if !errors.As(err, &wire) || wire.Code != proto.CodeConflict {
			t.Fatalf("lower epoch error=%v", err)
		}
	}
	if err := restarted.acceptControllerEpoch(9); err != nil {
		t.Fatalf("equal-epoch idempotent retry: %v", err)
	}
}

func TestCorruptControllerEpochFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controller-epoch.cbor")
	if err := persistControllerEpoch(path, 3); err != nil {
		t.Fatal(err)
	}
	// Truncation is the forbidden crash result: it must not silently reset the
	// durable fence and admit epoch 1 after restart.
	if err := os.WriteFile(path, []byte{0xff}, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadControllerEpoch(path); err == nil {
		t.Fatal("corrupt controller epoch was silently accepted")
	}
}
