package control

import (
	"context"
	"database/sql"
	"errors"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// The transcript mirror. The node's transcript session is the authoritative
// record of a run, but it lives on the node and goes with the workspace when
// it sleeps or moves. The node ships every record it appends to the control
// plane too (AgentReportTranscript), and the control plane keeps a bounded
// per-agent copy in the transcripts table so a reader can follow a
// conversation from any device without touching, and so without waking, the
// workspace. Records carry an agent-wide index that is monotonic across runs;
// eviction is oldest-first once an agent holds more than
// MaxTranscriptBytesPerAgent, and a reader below the oldest retained index is
// told so with a gap rather than handed a shorter conversation.

// transcriptDefaultPage is the page size when the reader does not say.
const transcriptDefaultPage = 200

// mirrorTranscriptLocked appends a batch of records to the agent's mirror.
// It validates the batch, assigns indices and evicts oldest-first, and
// commits the rows with the agent (which carries the bounds) in one
// transaction with events. Caller holds c.mu.
func (c *Control) mirrorTranscriptLocked(a *proto.Agent, run *proto.AgentRun, chunks []proto.TranscriptChunk, events []*proto.Event) error {
	var total int
	for i := range chunks {
		total += len(chunks[i].Data)
		if len(chunks[i].Data) > proto.MaxACPTranscriptFrame+4096 {
			return proto.Err(proto.CodeBadRequest, "transcript chunk seq %d is %d bytes; the node bounds frames at %d", chunks[i].Seq, len(chunks[i].Data), proto.MaxACPTranscriptFrame)
		}
	}
	if total > proto.MaxTranscriptReportBytes+64<<10 {
		return proto.Err(proto.CodeBadRequest, "transcript report is %d bytes; the bound is %d", total, proto.MaxTranscriptReportBytes)
	}
	if len(chunks) == 0 {
		return c.persistAgent(a, events...)
	}
	first, oldFirst := a.TranscriptNext, a.TranscriptFirst
	a.TranscriptNext += uint64(len(chunks))
	a.TranscriptBytes += int64(total)
	a.UpdatedAt = c.now().UnixMilli()
	trimAgentRuns(a)
	newFirst := a.TranscriptFirst
	newBytes := a.TranscriptBytes
	err := c.transact(func(tx *eventlog.Tx) error {
		for i := range chunks {
			ch := &chunks[i]
			if _, err := tx.Exec(`INSERT OR REPLACE INTO transcripts(agent, idx, run, seq, stream, at, data) VALUES(?,?,?,?,?,?,?)`,
				a.ID, int64(first)+int64(i), run.ID, int64(ch.Seq), int(ch.Stream), ch.At, ch.Data); err != nil {
				return err
			}
		}
		// Evict oldest-first until the agent is back under its budget. The
		// budget is generous relative to a batch, so this loop is short and
		// most batches never enter it.
		for newBytes > c.opts.MaxTranscriptBytesPerAgent && newFirst < first {
			rows, err := tx.Query(`SELECT idx, length(data) FROM transcripts WHERE agent=? AND idx>=? ORDER BY idx LIMIT 256`, a.ID, int64(newFirst))
			if err != nil {
				return err
			}
			var (
				evictTo   = newFirst
				evictSize int64
				n         int
			)
			for rows.Next() {
				var idx, size int64
				if err := rows.Scan(&idx, &size); err != nil {
					rows.Close()
					return err
				}
				n++
				evictTo = uint64(idx) + 1
				evictSize += size
				if newBytes-evictSize <= c.opts.MaxTranscriptBytesPerAgent {
					break
				}
			}
			if err := rows.Close(); err != nil {
				return err
			}
			if n == 0 {
				// Rows below first are already gone (a prune raced us);
				// account for what the table actually holds.
				newFirst = first
				newBytes = int64(total)
				break
			}
			if _, err := tx.Exec(`DELETE FROM transcripts WHERE agent=? AND idx<?`, a.ID, int64(evictTo)); err != nil {
				return err
			}
			newFirst = evictTo
			newBytes -= evictSize
		}
		a.TranscriptFirst, a.TranscriptBytes = newFirst, newBytes
		if _, err := tx.Exec(`INSERT OR REPLACE INTO agents(id, data) VALUES(?,?)`, a.ID, proto.MustMarshal(a)); err != nil {
			return err
		}
		return nil
	}, events)
	if err != nil {
		return err
	}
	metrics.AgentTranscriptRecords.Add(uint64(len(chunks)))
	metrics.AgentTranscriptBytes.Add(uint64(total))
	if evicted := a.TranscriptFirst - oldFirst; evicted > 0 {
		metrics.AgentTranscriptEvicted.Add(evicted)
	}
	c.wakeTranscriptWaitersLocked(a.ID)
	return nil
}

// wakeTranscriptWaitersLocked releases every reader parked on the agent.
// Readers re-check and re-park, so a spurious wake costs one query. Caller
// holds c.mu.
func (c *Control) wakeTranscriptWaitersLocked(agent string) {
	if ch := c.transcriptWaiters[agent]; ch != nil {
		close(ch)
		delete(c.transcriptWaiters, agent)
	}
}

// TranscriptWait returns a channel that is closed once the agent has a
// record at or past index after, or once the agent can never have one
// (terminal, or gone). It is how a tail follows the mirror without polling
// the database. Callers authorize the read first.
func (c *Control) TranscriptWait(agent string, after uint64) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	a := c.agents[agent]
	if a == nil || a.TranscriptNext > after || agentTerminal(a.Status) {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	ch := c.transcriptWaiters[agent]
	if ch == nil {
		ch = make(chan struct{})
		c.transcriptWaiters[agent] = ch
	}
	return ch
}

// agentTranscript is the agent.transcript op: one page of the mirror.
func (c *Control) agentTranscript(ctx context.Context, subject Subject, req *proto.AgentTranscriptReq) (*proto.AgentTranscriptRes, error) {
	if req.ID == "" {
		return nil, proto.Err(proto.CodeBadRequest, "agent id is required")
	}
	a, err := c.agentCopy(req.ID)
	if err != nil {
		return nil, err
	}
	if err := c.agentAuthorize(ctx, subject, a, ActionRead); err != nil {
		return nil, err
	}
	return c.readTranscript(ctx, a, req.From, req.Limit)
}

// readTranscript pages the mirror for an already-authorized agent copy.
func (c *Control) readTranscript(ctx context.Context, a *proto.Agent, from uint64, limit int) (*proto.AgentTranscriptRes, error) {
	if limit <= 0 {
		limit = transcriptDefaultPage
	}
	if limit > proto.MaxTranscriptPage {
		limit = proto.MaxTranscriptPage
	}
	res := &proto.AgentTranscriptRes{Records: []proto.TranscriptRecord{}, Next: from}
	if from < a.TranscriptFirst {
		res.Gap = &proto.TranscriptGap{From: from, To: a.TranscriptFirst}
		res.Next = a.TranscriptFirst
	}
	rows, err := c.db.QueryContext(ctx, `SELECT idx, run, seq, stream, at, data FROM transcripts WHERE agent=? AND idx>=? ORDER BY idx LIMIT ?`, a.ID, int64(res.Next), limit)
	if err != nil {
		return nil, proto.Err(proto.CodeInternal, "transcript read: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			rec         proto.TranscriptRecord
			idx, seq    int64
			stream      int
			data        []byte
			runID       sql.NullString
			previousIdx = res.Next
		)
		if err := rows.Scan(&idx, &runID, &seq, &stream, &rec.At, &data); err != nil {
			return nil, proto.Err(proto.CodeInternal, "transcript scan: %v", err)
		}
		rec.Index, rec.Seq, rec.Stream, rec.Data, rec.Run = uint64(idx), uint64(seq), uint8(stream), data, runID.String
		// A hole inside the retained range means a concurrent eviction moved
		// first past us; report it rather than silently skipping.
		if rec.Index > previousIdx && len(res.Records) == 0 && res.Gap == nil {
			res.Gap = &proto.TranscriptGap{From: previousIdx, To: rec.Index}
		}
		res.Records = append(res.Records, rec)
		res.Next = rec.Index + 1
	}
	if err := rows.Err(); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, proto.Err(proto.CodeInternal, "transcript read: %v", err)
	}
	res.Done = agentTerminal(a.Status) && res.Next >= a.TranscriptNext
	return res, nil
}
