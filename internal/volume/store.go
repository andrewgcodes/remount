package volume

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const stateVersion = 2

type volumeRecord struct {
	Volume   Volume    `json:"volume"`
	Versions []Version `json:"versions"`
}

type operation struct {
	Kind            string      `json:"kind"`
	Fingerprint     string      `json:"fingerprint"`
	ResourceKey     string      `json:"resource_key,omitempty"`
	MountAttempted  bool        `json:"mount_attempted,omitempty"`
	ReplaceExisting bool        `json:"replace_existing,omitempty"`
	Done            bool        `json:"done"`
	Volume          *Volume     `json:"volume,omitempty"`
	Attachment      *Attachment `json:"attachment,omitempty"`
	CompletedAt     time.Time   `json:"completed_at,omitempty"`
}

type catalogState struct {
	Format       int                     `json:"format"`
	Volumes      map[string]volumeRecord `json:"volumes"`
	Attachments  map[string]Attachment   `json:"attachments"`
	Fences       map[string]uint64       `json:"workspace_fences"`
	FenceUpdated map[string]time.Time    `json:"workspace_fence_updated"`
	Operations   map[string]operation    `json:"operations"`
	Counters     Stats                   `json:"counters"`
}

func newState() catalogState {
	return catalogState{
		Format:       stateVersion,
		Volumes:      make(map[string]volumeRecord),
		Attachments:  make(map[string]Attachment),
		Fences:       make(map[string]uint64),
		FenceUpdated: make(map[string]time.Time),
		Operations:   make(map[string]operation),
	}
}

func loadState(filename string) (catalogState, error) {
	body, err := os.ReadFile(filename)
	if errors.Is(err, os.ErrNotExist) {
		return newState(), nil
	}
	if err != nil {
		return catalogState{}, err
	}
	var state catalogState
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&state); err != nil {
		return catalogState{}, fmt.Errorf("volume: decode state: %w", err)
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return catalogState{}, errors.New("volume: trailing state data")
	}
	if state.Format != 1 && state.Format != stateVersion {
		return catalogState{}, fmt.Errorf("volume: unsupported state format %d", state.Format)
	}
	if state.Volumes == nil || state.Attachments == nil || state.Fences == nil || state.Operations == nil {
		return catalogState{}, errors.New("volume: incomplete state")
	}
	if state.Format == 1 {
		if state.FenceUpdated == nil {
			state.FenceUpdated = make(map[string]time.Time)
		}
		for key := range state.Fences {
			if _, ok := state.FenceUpdated[key]; !ok {
				state.FenceUpdated[key] = time.Unix(0, 0).UTC()
			}
		}
	} else if state.FenceUpdated == nil {
		return catalogState{}, errors.New("volume: incomplete workspace fence timestamps")
	}
	state.Format = stateVersion
	return state, nil
}

func saveState(filename string, state catalogState) error {
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".volumes-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	keep := false
	defer func() {
		_ = tmp.Close()
		if !keep {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if n, err := tmp.Write(body); err != nil || n != len(body) {
		if err == nil {
			err = errors.New("short state write")
		}
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filename); err != nil {
		return err
	}
	keep = true
	return syncParentDir(dir)
}

func cloneState(state catalogState) (catalogState, error) {
	body, err := json.Marshal(state)
	if err != nil {
		return catalogState{}, err
	}
	var clone catalogState
	if err := json.Unmarshal(body, &clone); err != nil {
		return catalogState{}, err
	}
	return clone, nil
}

func sortedVolumes(state catalogState, tenant string) []Volume {
	result := make([]Volume, 0)
	for _, record := range state.Volumes {
		if record.Volume.Tenant == tenant {
			result = append(result, record.Volume)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
