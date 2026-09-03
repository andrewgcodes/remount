package temporalexample

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"remount.dev/remount/api"
	remountclient "remount.dev/remount/client"
)

type fakeHeartbeats struct {
	resume   *Progress
	recorded []Progress
}

func (f *fakeHeartbeats) Resume(_ context.Context, target *Progress) (bool, error) {
	if f.resume == nil {
		return false, nil
	}
	*target = *f.resume
	return true, nil
}

func (f *fakeHeartbeats) Record(_ context.Context, progress Progress) {
	f.recorded = append(f.recorded, progress)
}

type fakeSession struct {
	id     string
	next   uint64
	chunks chan remountclient.Chunk
	exit   *api.ExitInfo
	err    error
}

func (s *fakeSession) ID() string                         { return s.id }
func (s *fakeSession) Next() uint64                       { return s.next }
func (s *fakeSession) Chunks() <-chan remountclient.Chunk { return s.chunks }
func (s *fakeSession) Exit() *api.ExitInfo                { return s.exit }
func (s *fakeSession) Err() error                         { return s.err }

type fakeRemount struct {
	session    Session
	open       *api.SessionOpenRequest
	attachWS   string
	attachID   string
	attachFrom uint64
	closed     bool
}

func (f *fakeRemount) Open(_ context.Context, request api.SessionOpenRequest) (Session, error) {
	f.open = &request
	return f.session, nil
}

func (f *fakeRemount) Attach(_ context.Context, workspace, session string, from uint64) (Session, error) {
	f.attachWS, f.attachID, f.attachFrom = workspace, session, from
	return f.session, nil
}

func (f *fakeRemount) Close() error { f.closed = true; return nil }

func closedSession(id string, next uint64, exit int) *fakeSession {
	chunks := make(chan remountclient.Chunk)
	close(chunks)
	return &fakeSession{id: id, next: next, chunks: chunks, exit: &api.ExitInfo{Code: exit}}
}

func TestRunStepStartsOnceWithStableIdempotency(t *testing.T) {
	remote := &fakeRemount{session: closedSession("s_1", 4, 7)}
	heartbeats := &fakeHeartbeats{}
	activity := Activities{
		NewRemount: func() (Remount, error) { return remote, nil },
		heartbeats: heartbeats,
		key:        func(context.Context) string { return "temporal_stable" },
	}
	input := StepInput{Workspace: "ws_1", Program: []string{"sh", "-c", "exit 7"}, Cwd: "/work", Env: map[string]string{"MODE": "test"}}
	result, err := activity.RunStep(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Session != "s_1" || result.Exit != 7 || result.Next != 4 {
		t.Fatalf("result = %+v", result)
	}
	if remote.open == nil || remote.open.IdempotencyKey != "temporal_stable" || !reflect.DeepEqual(remote.open.Program, input.Program) {
		t.Fatalf("open request = %+v", remote.open)
	}
	if !remote.closed || len(heartbeats.recorded) != 1 || heartbeats.recorded[0] != (Progress{Session: "s_1"}) {
		t.Fatalf("closed=%v heartbeats=%+v", remote.closed, heartbeats.recorded)
	}
}

func TestRunStepWorkerRestartReattachesFromHeartbeat(t *testing.T) {
	remote := &fakeRemount{session: closedSession("s_1", 12, 0)}
	heartbeats := &fakeHeartbeats{resume: &Progress{Session: "s_1", Next: 9}}
	activity := Activities{NewRemount: func() (Remount, error) { return remote, nil }, heartbeats: heartbeats}

	result, err := activity.RunStep(context.Background(), StepInput{Workspace: "ws_1", Program: []string{"make", "race"}})
	if err != nil {
		t.Fatal(err)
	}
	if remote.open != nil {
		t.Fatal("retry started a duplicate session")
	}
	if remote.attachWS != "ws_1" || remote.attachID != "s_1" || remote.attachFrom != 9 {
		t.Fatalf("attach = %q %q %d", remote.attachWS, remote.attachID, remote.attachFrom)
	}
	if result.Next != 12 || len(heartbeats.recorded) != 1 || heartbeats.recorded[0].Next != 9 {
		t.Fatalf("result=%+v heartbeats=%+v", result, heartbeats.recorded)
	}
}

func TestRunStepDoesNotHeartbeatPastExit(t *testing.T) {
	chunks := make(chan remountclient.Chunk, 2)
	chunks <- remountclient.Chunk{Seq: 8, Stream: api.StreamStdout, Data: []byte("done")}
	chunks <- remountclient.Chunk{Seq: 9, Stream: api.StreamExit}
	close(chunks)
	remote := &fakeRemount{session: &fakeSession{id: "s_1", next: 10, chunks: chunks, exit: &api.ExitInfo{}}}
	heartbeats := &fakeHeartbeats{resume: &Progress{Session: "s_1", Next: 8}}
	activity := Activities{NewRemount: func() (Remount, error) { return remote, nil }, heartbeats: heartbeats}

	if _, err := activity.RunStep(context.Background(), StepInput{Workspace: "ws_1", Program: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	if len(heartbeats.recorded) != 2 || heartbeats.recorded[0].Next != 8 || heartbeats.recorded[1].Next != 9 {
		// The consumed stdout advances the cursor, but consuming exit may not
		// advance it to 10: a retry must replay the terminal frame.
		t.Fatalf("heartbeats = %+v", heartbeats.recorded)
	}
}

func TestRunStepRejectsMissingCommandBeforeConnecting(t *testing.T) {
	activity := Activities{NewRemount: func() (Remount, error) { return nil, errors.New("should not connect") }}
	if _, err := activity.RunStep(context.Background(), StepInput{Workspace: "ws_1"}); err == nil {
		t.Fatal("missing command accepted")
	}
}
