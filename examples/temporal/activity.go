// Package temporalexample runs a durable command session from a Temporal
// Activity. Temporal owns orchestration and retry; Remount owns the computer,
// process, output log, and replay cursor.
package temporalexample

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"

	"remount.dev/remount/api"
	remountclient "remount.dev/remount/client"
)

// StepInput identifies one command in an existing Remount workspace.
type StepInput struct {
	Workspace string            `json:"workspace"`
	Program   []string          `json:"program"`
	Cwd       string            `json:"cwd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
}

// StepResult points at the authoritative Remount session and its terminal
// status. Output remains replayable from Remount instead of being copied into
// Temporal history.
type StepResult struct {
	Session string `json:"session"`
	Next    uint64 `json:"next"`
	Exit    int    `json:"exit"`
	Signal  string `json:"signal,omitempty"`
	Error   string `json:"error,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

// Progress is the bounded heartbeat state used to reattach after an Activity
// retry or worker restart.
type Progress struct {
	Session string `json:"session"`
	Next    uint64 `json:"next"`
}

// Session is the narrow durable-stream contract the Activity consumes.
type Session interface {
	ID() string
	Next() uint64
	Chunks() <-chan remountclient.Chunk
	Exit() *api.ExitInfo
	Err() error
}

// Remount is implemented by the public SDK adapter and by focused tests.
type Remount interface {
	Open(context.Context, api.SessionOpenRequest) (Session, error)
	Attach(context.Context, string, string, uint64) (Session, error)
	Close() error
}

type heartbeats interface {
	Resume(context.Context, *Progress) (bool, error)
	Record(context.Context, Progress)
}

type temporalHeartbeats struct{}

func (temporalHeartbeats) Resume(ctx context.Context, progress *Progress) (bool, error) {
	if !activity.HasHeartbeatDetails(ctx) {
		return false, nil
	}
	return true, activity.GetHeartbeatDetails(ctx, progress)
}

func (temporalHeartbeats) Record(ctx context.Context, progress Progress) {
	activity.RecordHeartbeat(ctx, progress)
}

// Activities contains dependencies for Temporal Activity registration.
type Activities struct {
	// NewRemount is injectable for tests. If nil, REMOUNT_SERVER and
	// REMOUNT_TOKEN are read by NewEnvironmentClient.
	NewRemount func() (Remount, error)
	// Logf receives session ids and cursors only; it must never log client
	// credentials or command environment.
	Logf       func(string, ...any)
	heartbeats heartbeats
	key        func(context.Context) string
}

// RunStep starts or reattaches one command. Its idempotency key is derived from
// Temporal's workflow and Activity IDs, so a crash before the first heartbeat
// cannot duplicate the process.
func (a *Activities) RunStep(ctx context.Context, input StepInput) (StepResult, error) {
	if input.Workspace == "" {
		return StepResult{}, errors.New("workspace is required")
	}
	if len(input.Program) == 0 || input.Program[0] == "" {
		return StepResult{}, errors.New("program is required")
	}
	newClient := a.NewRemount
	if newClient == nil {
		newClient = NewEnvironmentClient
	}
	remount, err := newClient()
	if err != nil {
		return StepResult{}, fmt.Errorf("connect Remount: %w", err)
	}
	defer remount.Close()

	hb := a.heartbeats
	if hb == nil {
		hb = temporalHeartbeats{}
	}
	var progress Progress
	resuming, err := hb.Resume(ctx, &progress)
	if err != nil {
		return StepResult{}, fmt.Errorf("decode heartbeat: %w", err)
	}

	var session Session
	if resuming {
		if progress.Session == "" {
			return StepResult{}, errors.New("heartbeat has no Remount session")
		}
		session, err = remount.Attach(ctx, input.Workspace, progress.Session, progress.Next)
	} else {
		key := a.key
		if key == nil {
			key = activityKey
		}
		session, err = remount.Open(ctx, api.SessionOpenRequest{
			WS: input.Workspace, Kind: api.SessionExec,
			Program: append([]string(nil), input.Program...), Cwd: input.Cwd,
			Env: cloneMap(input.Env), IdempotencyKey: key(ctx),
		})
	}
	if err != nil {
		return StepResult{}, fmt.Errorf("open Remount session: %w", err)
	}
	if resuming {
		// Keep the acknowledged consumer cursor. Session.Next may already
		// include frames buffered by the SDK but not consumed by this Activity.
		progress.Session = session.ID()
	} else {
		progress = Progress{Session: session.ID()}
	}
	hb.Record(ctx, progress)
	if a.Logf != nil {
		a.Logf("remount session ready session=%s next=%d reattached=%t", progress.Session, progress.Next, resuming)
	}
	heartbeatTicker := time.NewTicker(5 * time.Second)
	defer heartbeatTicker.Stop()

	for {
		select {
		case chunk, ok := <-session.Chunks():
			if !ok {
				if err := session.Err(); err != nil {
					return StepResult{}, fmt.Errorf("read Remount session: %w", err)
				}
				exit := session.Exit()
				if exit == nil {
					return StepResult{}, errors.New("Remount session ended without exit status")
				}
				return StepResult{Session: session.ID(), Next: session.Next(), Exit: exit.Code,
					Signal: exit.Signal, Error: exit.Error, Reason: exit.Reason}, nil
			}
			// Keep the last durable cursor at or before exit. If the worker
			// crashes after receiving exit but before returning, the retry must
			// replay that terminal frame; attaching after it cannot rediscover
			// the exit status.
			if chunk.Stream != api.StreamExit && chunk.Stream != api.StreamGap {
				progress.Next = chunk.Seq + 1
				hb.Record(ctx, progress)
			}
		case <-heartbeatTicker.C:
			// A silent command must still extend Temporal's heartbeat timeout.
			hb.Record(ctx, progress)
		case <-ctx.Done():
			return StepResult{}, ctx.Err()
		}
	}
}

func activityKey(ctx context.Context) string {
	info := activity.GetInfo(ctx)
	value := info.WorkflowExecution.ID + "\x00" + info.WorkflowExecution.RunID + "\x00" + info.ActivityID
	sum := sha256.Sum256([]byte(value))
	return "temporal_" + hex.EncodeToString(sum[:])
}

func cloneMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

type sdkSession struct{ *remountclient.Session }

func (s sdkSession) ID() string { return s.Session.ID }

type sdkClient struct{ *remountclient.Client }

func (c sdkClient) Open(ctx context.Context, request api.SessionOpenRequest) (Session, error) {
	session, err := c.Exec(ctx, request)
	if err != nil {
		return nil, err
	}
	return sdkSession{session}, nil
}

func (c sdkClient) Attach(ctx context.Context, workspace, sessionID string, from uint64) (Session, error) {
	session, err := c.Client.Attach(ctx, workspace, sessionID, from)
	if err != nil {
		return nil, err
	}
	return sdkSession{session}, nil
}
