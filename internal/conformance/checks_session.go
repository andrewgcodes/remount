package conformance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// ---- cursor output, input deduplication and gaps -------------------------

func checkSessInfoIsSeqZero(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	run, err := s.Exec(ctx, f, echoProgram(s.Target.workspaceOS(), "conformance")...)
	if err != nil {
		return err
	}
	if len(run.Chunks) == 0 {
		return failf("the session produced no chunks at all")
	}
	first := run.Chunks[0]
	if first.Seq != 0 {
		return failf("the first chunk of %s carried seq %d, not 0", run.Session, first.Seq)
	}
	var body ChunkBody
	if err := Unmarshal(first.Body, &body); err != nil {
		return failf("chunk 0 is not a ChunkBody: %v", err)
	}
	if body.St != ChunkInfo {
		return failf("chunk 0 of %s has stream type %d, not the info type %d; a replay from 0 could not reconstruct the header", run.Session, body.St, ChunkInfo)
	}
	var info SessionInfo
	if err := Unmarshal(body.D, &info); err != nil {
		return failf("the seq 0 payload is not a SessionInfo: %v", err)
	}
	if info.ID != run.Session {
		return failf("the info chunk names session %q while the open returned %q", info.ID, run.Session)
	}
	if info.WS != f.WS.ID {
		return failf("the info chunk names workspace %q, not %q", info.WS, f.WS.ID)
	}
	return nil
}

func checkSessExitIsLast(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	run, err := s.Exec(ctx, f, echoProgram(s.Target.workspaceOS(), "last")...)
	if err != nil {
		return err
	}
	for i, c := range run.Chunks {
		var body ChunkBody
		if err := Unmarshal(c.Body, &body); err != nil {
			return failf("chunk %d is not a ChunkBody: %v", c.Seq, err)
		}
		if body.St == ChunkExit && i != len(run.Chunks)-1 {
			return failf("the exit chunk appeared at position %d of %d; §8 guarantee 2 makes it the last chunk", i, len(run.Chunks))
		}
	}
	var last ChunkBody
	if err := Unmarshal(run.Chunks[len(run.Chunks)-1].Body, &last); err != nil {
		return err
	}
	if last.St != ChunkExit {
		return failf("the last chunk has stream type %d, not the exit type %d", last.St, ChunkExit)
	}
	// The log is closed after the exit chunk: nothing more may arrive.
	select {
	case extra := <-s.Control.Chunks(run.Session):
		return failf("a chunk at seq %d arrived after the exit chunk", extra.Seq)
	case <-time.After(300 * time.Millisecond):
	}
	return nil
}

func checkSessSeqDense(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	run, err := s.Exec(ctx, f, echoProgram(s.Target.workspaceOS(), "dense")...)
	if err != nil {
		return err
	}
	for i, c := range run.Chunks {
		if c.Seq != uint64(i) {
			return failf("chunk %d of %s carried seq %d; §8 guarantee 1 requires seq to start at 0 and increase by one", i, run.Session, c.Seq)
		}
	}
	return nil
}

func checkSessInputDeduplicated(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	var res SOpenRes
	open := SOpenReq{WS: f.WS.ID, Kind: "exec", Program: catProgram(s.Target.workspaceOS()), Stdin: true, Grant: &f.Grant, Idem: s.Idem("dedup")}
	if err := s.CallNode(ctx, f.Node, "s.open", open, &res); err != nil {
		return err
	}
	send := func(iseq uint64, data string, eof bool) error {
		return s.CallNode(ctx, f.Node, "s.input", SInputReq{S: res.S, ISeq: iseq, Data: []byte(data), EOF: eof, Grant: &f.Grant}, nil)
	}
	// The sequences start above 1 so the "at or below" half of the rule can
	// be probed with a lower nonzero value. iseq 0 is deliberately not used:
	// §8 does not say whether 0 means "sequence zero" or "unsequenced", and a
	// conformance suite must not fail an implementation on an ambiguity in
	// the document it is enforcing.
	if err := send(5, "alpha\n", false); err != nil {
		return err
	}
	// The same keystroke, retried after a notional dropped connection.
	if err := send(5, "alpha\n", false); err != nil {
		return failf("a retried input at the same iseq was rejected rather than dropped: %v", err)
	}
	// And one below the last applied sequence, which is also a retry.
	if err := send(3, "beta\n", false); err != nil {
		return failf("a retried input below the last applied iseq was rejected rather than dropped: %v", err)
	}
	if err := send(6, "", true); err != nil {
		return err
	}
	frames, err := s.Control.CollectSession(ctx, res.S)
	if err != nil {
		return err
	}
	run, err := decodeRun(res.S, frames)
	if err != nil {
		return err
	}
	if n := bytes.Count(run.Stdout, []byte("alpha")); n != 1 {
		return failf("the process echoed %d copies of one retried keystroke (stdout %q); §8 requires a node to drop an iseq equal to the last it applied", n, run.Stdout)
	}
	if bytes.Contains(run.Stdout, []byte("beta")) {
		return failf("input sent at iseq 3, below the last applied iseq 5, was applied (stdout %q); §8 drops any iseq at or below the last one", run.Stdout)
	}
	return nil
}

func checkSessReplayIsByteIdentical(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	run, err := s.Exec(ctx, f, echoProgram(s.Target.workspaceOS(), "replay-me")...)
	if err != nil {
		return err
	}
	var attach SOpenRes
	if err := s.CallNode(ctx, f.Node, "s.attach", SAttachReq{S: run.Session, From: 0, Grant: &f.Grant}, &attach); err != nil {
		return failf("s.attach from 0 on an exited session failed: %v", err)
	}
	frames, err := s.Control.CollectSession(ctx, run.Session)
	if err != nil {
		return failf("the replay never reached its exit chunk: %v", err)
	}
	replay, err := decodeRun(run.Session, frames)
	if err != nil {
		return err
	}
	if len(replay.Chunks) != len(run.Chunks) {
		return failf("the replay produced %d chunks where the live tail produced %d", len(replay.Chunks), len(run.Chunks))
	}
	for i := range run.Chunks {
		if run.Chunks[i].Seq != replay.Chunks[i].Seq || !bytes.Equal(run.Chunks[i].Body, replay.Chunks[i].Body) {
			return failf("replayed chunk %d differs from the live one; §8 guarantee 3 makes replay and live tail the same path", i)
		}
	}
	if !bytes.Equal(run.Stdout, replay.Stdout) {
		return failf("replayed stdout %q is not byte-identical to %q", replay.Stdout, run.Stdout)
	}
	return nil
}

func checkSessWaitReportsExit(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	var res SOpenRes
	open := SOpenReq{WS: f.WS.ID, Kind: "exec", Program: exitProgram(s.Target.workspaceOS(), 7), Grant: &f.Grant, Idem: s.Idem("wait")}
	if err := s.CallNode(ctx, f.Node, "s.open", open, &res); err != nil {
		return err
	}
	var wait SWaitRes
	if err := s.CallNode(ctx, f.Node, "s.wait", SWaitReq{S: res.S, TimeoutSec: 30, Grant: &f.Grant}, &wait); err != nil {
		return err
	}
	if !wait.Exited || wait.Exit == nil {
		return failf("s.wait returned exited=%v exit=%v for a command that ran to completion", wait.Exited, wait.Exit)
	}
	if wait.Exit.Code != 7 {
		return failf("s.wait reported exit code %d for a command that exited 7", wait.Exit.Code)
	}
	frames, err := s.Control.CollectSession(ctx, res.S)
	if err != nil {
		return err
	}
	run, err := decodeRun(res.S, frames)
	if err != nil {
		return err
	}
	if run.Exit.Code != wait.Exit.Code {
		return failf("the exit chunk reported code %d while s.wait reported %d", run.Exit.Code, wait.Exit.Code)
	}
	return nil
}

// checkSessEvictedReplayGaps is reached only when the target's session
// retention bound is small enough to overrun. The reference node's bound is
// 2 MiB of memory plus 128 MiB of spill per session with no knob to lower it,
// so this is honestly unavailable rather than quietly skipped.
func checkSessEvictedReplayGaps(_ context.Context, s *Session) error {
	return Unavailablef("target %q declares session eviction reachable, but the runner has no supported way to lower its retention bound", s.Target.Name)
}

func checkSessAttachBeyondRange(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	run, err := s.Exec(ctx, f, echoProgram(s.Target.workspaceOS(), "beyond")...)
	if err != nil {
		return err
	}
	beyond := uint64(len(run.Chunks)) + 1_000_000
	var attach SOpenRes
	err = s.CallNode(ctx, f.Node, "s.attach", SAttachReq{S: run.Session, From: beyond, Grant: &f.Grant}, &attach)
	if err != nil {
		// Refusing is a legitimate answer; inventing output is not.
		if code := CodeOf(err); !KnownCode(code) {
			return failf("an attach past the produced range was refused with the non-stable code %q (%v)", code, err)
		}
		return nil
	}
	select {
	case c := <-s.Control.Chunks(run.Session):
		var body ChunkBody
		if err := Unmarshal(c.Body, &body); err != nil {
			return err
		}
		if body.St == ChunkStdout || body.St == ChunkStderr {
			return failf("attaching at seq %d, past everything the session produced, delivered output at seq %d", beyond, c.Seq)
		}
	case <-time.After(500 * time.Millisecond):
	}
	if attach.Next > beyond {
		return failf("s.attach reported live beginning at seq %d, past the requested %d, without emitting a gap", attach.Next, beyond)
	}
	return nil
}

// ---- snapshot identity, restore, move and source retention ---------------

// seedFile is written into a workspace so a snapshot has content whose
// survival can be asserted after a restore or a move.
const seedFile = "conformance-seed.txt"

func seedContent(tag string) []byte { return []byte("conformance seed " + tag + "\n") }

func (s *Session) seed(ctx context.Context, f *fixture, tag string) error {
	return s.CallNode(ctx, f.Node, "fs.write", FSWriteReq{
		WS: f.WS.ID, Path: seedFile, Data: seedContent(tag), Grant: &f.Grant, Idem: s.Idem("seed"),
	}, nil)
}

func (s *Session) readSeed(ctx context.Context, f *fixture) ([]byte, error) {
	var res FSReadRes
	err := s.CallNode(ctx, f.Node, "fs.read", FSReadReq{WS: f.WS.ID, Path: seedFile, Grant: &f.Grant}, &res)
	return res.Data, err
}

// snapshot takes one snapshot, retrying while the implementation reports a
// rate limit. Bounding how often a workspace may be archived is a resource
// decision `resource_exhausted` exists to express; treating it as a broken
// contract would make the suite reject a legitimate implementation.
func (s *Session) snapshot(ctx context.Context, f *fixture, upload bool) (WSSnapshotRes, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		var res WSSnapshotRes
		err := s.CallNode(ctx, f.Node, "ws.snapshot", WSSnapshotReq{
			WS: f.WS.ID, Upload: upload, Grant: &f.Grant, Idem: s.Idem("snap"),
		}, &res)
		if !IsCode(err, "resource_exhausted") || time.Now().After(deadline) {
			return res, err
		}
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func checkSnapDeterministicID(ctx context.Context, s *Session) error {
	f, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-snap-det"})
	if err != nil {
		return err
	}
	if err := s.seed(ctx, f, "determinism"); err != nil {
		return err
	}
	first, err := s.snapshot(ctx, f, false)
	if err != nil {
		return err
	}
	second, err := s.snapshot(ctx, f, false)
	if err != nil {
		return err
	}
	if first.Artifact == "" {
		return failf("ws.snapshot returned no artifact id")
	}
	if first.Artifact != second.Artifact {
		return failf("two snapshots of an unchanged tree produced %s and %s; §10 says the same tree always produces the same id", first.Artifact, second.Artifact)
	}
	// Changing the tree must change the id, or "deterministic" would be
	// satisfied by returning a constant.
	if err := s.seed(ctx, f, "determinism-changed"); err != nil {
		return err
	}
	third, err := s.snapshot(ctx, f, false)
	if err != nil {
		return err
	}
	if third.Artifact == first.Artifact {
		return failf("changing a file left the snapshot id at %s, so the id does not identify the tree", third.Artifact)
	}
	return nil
}

func checkSnapDigestIsIdentity(ctx context.Context, s *Session) error {
	f, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-snap-digest"})
	if err != nil {
		return err
	}
	if err := s.seed(ctx, f, "digest"); err != nil {
		return err
	}
	snap, err := s.snapshot(ctx, f, true)
	if err != nil {
		return err
	}
	want, err := digestOf(snap.Artifact)
	if err != nil {
		return err
	}
	status, body, err := s.GetArtifact(ctx, snap.Artifact)
	if err != nil {
		return err
	}
	if status != 200 {
		return failf("GET of the artifact this run just uploaded answered %d", status)
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != want {
		return failf("the bytes served for %s hash to %s; an artifact id is the digest of its bytes", snap.Artifact, hex.EncodeToString(sum[:]))
	}
	return nil
}

func checkSnapRestoreServesTree(ctx context.Context, s *Session) error {
	src, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-snap-src"})
	if err != nil {
		return err
	}
	if err := s.seed(ctx, src, "restore"); err != nil {
		return err
	}
	snap, err := s.snapshot(ctx, src, true)
	if err != nil {
		return err
	}
	dst, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-snap-dst", RestoreFrom: snap.Artifact})
	if err != nil {
		return err
	}
	got, err := s.readSeed(ctx, dst)
	if err != nil {
		return failf("the restored workspace could not serve the file the snapshot carried: %v", err)
	}
	if !bytes.Equal(got, seedContent("restore")) {
		return failf("the restored workspace served %q where the source held %q", got, seedContent("restore"))
	}
	return nil
}

func checkSnapLiveIsNotAuthoritative(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	snap, err := s.snapshot(ctx, f, false)
	if err != nil {
		return err
	}
	if snap.Consistency != "live" {
		return failf("an ordinary snapshot reported consistency %q, not %q", snap.Consistency, "live")
	}
	if snap.Authoritative {
		return failf("an ordinary snapshot reported itself authoritative; §12 item 12 says a live snapshot cannot replace authoritative recovery state")
	}
	return nil
}

func checkSnapUnknownArtifact(ctx context.Context, s *Session) error {
	absent := "art_sha256:" + strings.Repeat("ab", 32)
	status, _, err := s.GetArtifact(ctx, absent)
	if err != nil {
		return err
	}
	if status != 404 {
		return failf("GET of an artifact that does not exist answered %d, not 404", status)
	}
	return nil
}

func checkSnapDigestMismatchRefused(ctx context.Context, s *Session) error {
	body := []byte("conformance: these bytes do not hash to the id they are stored under")
	// An id whose digest is deliberately not the body's.
	wrong := "art_sha256:" + strings.Repeat("cd", 32)
	status, res, err := s.PutArtifact(ctx, wrong, body)
	if err != nil {
		return err
	}
	if status >= 200 && status < 300 {
		return failf("a blob whose digest does not match its id was published (%d %s)", status, strings.TrimSpace(string(res)))
	}
	status, _, err = s.GetArtifact(ctx, wrong)
	if err != nil {
		return err
	}
	if status != 404 {
		return failf("the refused upload is nonetheless retrievable: GET answered %d", status)
	}
	return nil
}

func checkSnapMoveCarriesTree(ctx context.Context, s *Session) error {
	f, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-move"})
	if err != nil {
		return err
	}
	if err := s.seed(ctx, f, "move"); err != nil {
		return err
	}
	before := f.WS.Generation
	if err := s.Call(ctx, "ws.move", WSMoveReq{ID: f.WS.ID, Idem: s.Idem("move")}, nil); err != nil {
		return err
	}
	if err := s.Refresh(ctx, f); err != nil {
		return err
	}
	if f.WS.Generation <= before {
		return failf("a move left the generation at %d", f.WS.Generation)
	}
	got, err := s.readSeed(ctx, f)
	if err != nil {
		return failf("the moved workspace could not serve the file it held before the move: %v", err)
	}
	if !bytes.Equal(got, seedContent("move")) {
		return failf("the moved workspace serves %q where it held %q; §10 says the filesystem travels", got, seedContent("move"))
	}
	return nil
}

// checkSnapAuthoritativeQuiesces is reached only when the target declares a
// backend that can quiesce. It is written as a real assertion so that a
// target which declares the prerequisite is actually held to it.
func checkSnapAuthoritativeQuiesces(ctx context.Context, s *Session) error {
	f, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-quiesce"})
	if err != nil {
		return err
	}
	if err := s.seed(ctx, f, "quiesce"); err != nil {
		return err
	}
	var res WSSnapshotRes
	err = s.CallNode(ctx, f.Node, "ws.snapshot", WSSnapshotReq{
		WS: f.WS.ID, Upload: true, Authoritative: true, Grant: &f.Grant, Idem: s.Idem("quiesce"),
	}, &res)
	if err != nil {
		return failf("an authoritative snapshot failed: %v", err)
	}
	if res.Consistency != "quiesced" {
		return failf("an authoritative snapshot reported consistency %q, not %q", res.Consistency, "quiesced")
	}
	if !res.Authoritative {
		return failf("a successful authoritative snapshot did not report itself authoritative")
	}
	return nil
}

func digestOf(artifact string) (string, error) {
	const prefix = "art_sha256:"
	if !strings.HasPrefix(artifact, prefix) {
		return "", fmt.Errorf("artifact id %q does not carry the %q prefix §10 requires", artifact, prefix)
	}
	hexPart := strings.TrimPrefix(artifact, prefix)
	if len(hexPart) != 64 {
		return "", fmt.Errorf("artifact id %q does not carry a 64-hex sha256 digest", artifact)
	}
	if _, err := hex.DecodeString(hexPart); err != nil {
		return "", fmt.Errorf("artifact id %q is not hexadecimal: %w", artifact, err)
	}
	return hexPart, nil
}

// checkSessStdoutReachesTheClient is CONF-SESS-009.
//
// Every other row in this category asserts something about the *shape* of the
// log: that seq 0 is the info chunk, that sequences are dense, that the exit
// chunk is last, that a replay matches the live tail. All of those hold
// vacuously for a session that produces no output at all, which is how an
// implementation whose exec never delivers stdout was observed passing 50 of
// 52 required rows. §8 calls a session "an append-only log of chunks" and
// defines st=1 as "the process wrote this"; if those bytes do not arrive, the
// implementation has not implemented sessions, whatever its sequence numbers
// look like.
//
// The assertion is deliberately about bytes rather than about non-emptiness:
// output that arrives truncated, re-encoded, or on the wrong stream is the same
// defect as output that never arrives.
func checkSessStdoutReachesTheClient(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	// Spaces and punctuation so that an implementation which splits, trims or
	// re-quotes the payload fails here rather than in a caller's terminal.
	const want = "conf-sess-009: stdout must arrive verbatim."
	run, err := s.Exec(ctx, f, echoProgram(s.Target.workspaceOS(), want)...)
	if err != nil {
		return err
	}
	// An exec that failed to start would otherwise be indistinguishable from
	// one that ran and delivered nothing, and they are different defects.
	if run.Exit.Code != 0 {
		return failf("the program exited %d (%s), so this row cannot judge stdout delivery",
			run.Exit.Code, run.Exit.Error)
	}
	if len(run.Stderr) != 0 {
		return failf("the program wrote %q to stderr; /bin/echo of a literal must not", run.Stderr)
	}
	got := strings.TrimRight(string(run.Stdout), "\r\n")
	if got != want {
		return failf("stdout delivered %q, want %q: §8 st=1 chunks carry what the process wrote, byte for byte", got, want)
	}
	// The bytes arrived, but they must have arrived *as stdout*. An
	// implementation that folds every stream into one would pass the
	// comparison above while making stderr and stdout indistinguishable.
	var stdoutChunks int
	for _, frame := range run.Chunks {
		var body ChunkBody
		if err := Unmarshal(frame.Body, &body); err != nil {
			return failf("chunk %d is not a ChunkBody: %v", frame.Seq, err)
		}
		if body.St == ChunkStdout {
			stdoutChunks++
		}
	}
	if stdoutChunks == 0 {
		return failf("the payload arrived but no chunk was tagged st=1, so stdout is not distinguishable from another stream")
	}
	return nil
}
