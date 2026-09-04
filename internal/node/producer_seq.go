package node

import (
	"errors"
	"fmt"
	"os"

	"remount.dev/remount/internal/proto"
)

// producer-seq.cbor holds the highest producer sequence this node has assigned
// to an event. The node's own event store is in memory, so without this a
// restart would number its events from one again while control still holds
// the watermark from the previous life; every post-restart event would then
// collide with a sequence control has already recorded, and control's dedupe
// would answer "changed" to each of them forever. Continuing from the persisted
// value keeps one identity's sequence space monotonic across restarts.

func loadProducerSeq(path string) (uint64, error) {
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var state struct {
		Seq uint64 `cbor:"seq"`
	}
	if err := proto.Unmarshal(payload, &state); err != nil {
		return 0, fmt.Errorf("node: corrupt producer sequence watermark: %w", err)
	}
	return state.Seq, nil
}

// persistProducerSeq writes the watermark through a rename so a crash leaves
// either the old value or the new one, never a torn file. It does not fsync:
// losing the last few increments on power loss is recovered by the forwarder's
// collision handling, which is the same mechanism this file makes rare.
func persistProducerSeq(path string, seq uint64) error {
	payload, err := proto.Marshal(struct {
		Seq uint64 `cbor:"seq"`
	}{Seq: seq})
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
