package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	pathpkg "path"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

const (
	consoleFileLimit       = 1 << 20
	consoleCollectionLimit = 1000
)

type consoleNode struct {
	ID          string            `json:"id"`
	State       string            `json:"state"`
	Labels      map[string]string `json:"labels"`
	Assignments int               `json:"assignments"`
	Capacity    int               `json:"capacity"`
	LastSeen    string            `json:"lastSeen"`
}

type consolePool struct {
	ID      string `json:"id"`
	Vendor  string `json:"vendor"`
	Desired int    `json:"desired"`
	Actual  int    `json:"actual"`
	Min     int    `json:"min"`
	Max     int    `json:"max"`
	State   string `json:"state"`
}

type consoleWorkspace struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Tenant     string `json:"tenant"`
	State      string `json:"state"`
	Node       string `json:"node,omitempty"`
	Generation uint64 `json:"generation"`
	UpdatedAt  string `json:"updatedAt"`
}

type consoleSession struct {
	ID        string   `json:"id"`
	Kind      string   `json:"kind"`
	Command   []string `json:"command,omitempty"`
	StartedAt string   `json:"startedAt"`
	EndedAt   string   `json:"endedAt,omitempty"`
	Next      uint64   `json:"next"`
	Earliest  uint64   `json:"earliest"`
}

type consoleSnapshot struct {
	ID         string `json:"id"`
	Artifact   string `json:"artifact"`
	Generation uint64 `json:"generation"`
	CreatedAt  string `json:"createdAt"`
	Bytes      int64  `json:"bytes"`
}

type consoleGeneration struct {
	Generation uint64 `json:"generation"`
	Node       string `json:"node,omitempty"`
	State      string `json:"state"`
	At         string `json:"at"`
	EventSeq   uint64 `json:"eventSeq"`
}

type consoleWorkspaceDetail struct {
	consoleWorkspace
	Spec        consoleWorkspaceSpec `json:"spec"`
	Sessions    []consoleSession     `json:"sessions"`
	Snapshots   []consoleSnapshot    `json:"snapshots"`
	Generations []consoleGeneration  `json:"generations"`
}

type consoleWorkspaceSpec struct {
	Image    string            `json:"image,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
	Security string            `json:"security,omitempty"`
}

type consoleEvent struct {
	Seq        uint64         `json:"seq"`
	At         string         `json:"at"`
	Type       string         `json:"type"`
	Tenant     string         `json:"tenant"`
	Workspace  string         `json:"workspace,omitempty"`
	Session    string         `json:"session,omitempty"`
	Principal  string         `json:"principal,omitempty"`
	Credential string         `json:"credential,omitempty"`
	Payload    map[string]any `json:"payload,omitempty"`
}

type consoleApproval struct {
	ID        string   `json:"id"`
	Agent     string   `json:"agent,omitempty"`
	Workspace string   `json:"workspace"`
	Tenant    string   `json:"tenant"`
	Kind      string   `json:"kind"`
	Status    string   `json:"status"`
	Prompt    string   `json:"prompt"`
	Options   []string `json:"options,omitempty"`
	CreatedAt string   `json:"createdAt"`
}

func millis(value int64) string {
	if value <= 0 {
		return ""
	}
	return time.UnixMilli(value).UTC().Format(time.RFC3339Nano)
}

func renderConsoleWorkspace(ws proto.Workspace) consoleWorkspace {
	return consoleWorkspace{ID: ws.ID, Name: ws.Spec.Name, Tenant: ws.Tenant, State: ws.State, Node: ws.Node,
		Generation: ws.Generation, UpdatedAt: millis(ws.UpdatedAt)}
}

func hasConsoleRole(subject control.Subject, role string) bool {
	for _, candidate := range subject.Roles {
		if candidate == role || candidate == "admin" {
			return true
		}
	}
	return false
}

// consoleClient performs the browser surface's explicit role check before an
// in-process SDK client is acquired. Protocol authorization remains the final
// authority; this extra boundary prevents an agent token from becoming an
// operator merely because it can mutate its own workspace through the SDK.
func (s *Server) consoleClient(w http.ResponseWriter, r *http.Request, mutation bool) (*client.Client, control.Subject, func()) {
	tok, _, ok := credential(r, false)
	if !ok && (s.opts.Token != "" || s.opts.Authenticator != nil) {
		writeError(w, proto.Err(proto.CodeUnauthorized, "missing credential"))
		return nil, control.Subject{}, nil
	}
	subject := control.Subject{ID: "local", Tenant: "*", Roles: []string{"operator", "admin"}}
	shared := s.opts.Token != "" && subtle.ConstantTimeCompare([]byte(tok), []byte(s.opts.Token)) == 1
	if !shared && s.opts.Authenticator != nil {
		var err error
		subject, err = s.opts.Authenticator.Authenticate(r.Context(), control.Credential{Token: tok})
		if err != nil {
			writeError(w, proto.Err(proto.CodeUnauthorized, "invalid credential"))
			return nil, control.Subject{}, nil
		}
	}
	if mutation {
		if !hasConsoleRole(subject, "operator") {
			writeError(w, proto.Err(proto.CodeDenied, "operator role required"))
			return nil, control.Subject{}, nil
		}
		if r.Header.Get(idempotencyHeader) == "" && !isUpgrade(r) {
			badRequest(w, "Idempotency-Key is required for console mutations")
			return nil, control.Subject{}, nil
		}
	}
	cl, release, err := s.clients.acquire(r.Context(), tok)
	if err != nil {
		writeError(w, err)
		return nil, control.Subject{}, nil
	}
	metrics.HTTPRequests.Inc()
	return cl, subject, release
}

func (s *Server) consoleRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/console/fleet", s.handleConsoleFleet)
	mux.HandleFunc("POST /v1/console/workspaces", s.handleConsoleWorkspaceCreate)
	mux.HandleFunc("GET /v1/console/workspaces/{id}", s.handleConsoleWorkspaceGet)
	mux.HandleFunc("POST /v1/console/workspaces/{id}/{action}", s.handleConsoleWorkspaceAction)
	mux.HandleFunc("GET /v1/console/workspaces/{id}/files", s.handleConsoleFiles)
	mux.HandleFunc("GET /v1/console/workspaces/{id}/file", s.handleConsoleFileRead)
	mux.HandleFunc("PUT /v1/console/workspaces/{id}/file", s.handleConsoleFileWrite)
	mux.HandleFunc("GET /v1/console/workspaces/{id}/terminal", s.handleConsoleTerminal)
	mux.HandleFunc("GET /v1/console/events", s.handleConsoleEvents)
	mux.HandleFunc("GET /v1/console/approvals", s.handleConsoleApprovals)
}

func (s *Server) handleConsoleFleet(w http.ResponseWriter, r *http.Request) {
	cl, _, release := s.consoleClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	nodes, err := cl.ListNodes(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	pools, err := cl.ListPools(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	workspaces, err := cl.ListWorkspaces(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if len(nodes) > consoleCollectionLimit || len(pools) > consoleCollectionLimit || len(workspaces) > consoleCollectionLimit {
		writeError(w, proto.Err(proto.CodeResourceExhausted, "console fleet observation exceeds %d rows per collection", consoleCollectionLimit))
		return
	}
	outNodes := make([]consoleNode, 0, len(nodes))
	for _, node := range nodes {
		state := "offline"
		if node.Online {
			state = "online"
		}
		outNodes = append(outNodes, consoleNode{ID: node.ID, State: state, Labels: node.Labels,
			Assignments: len(node.Workspaces), Capacity: node.Info.CPU, LastSeen: millis(node.LastSeen)})
	}
	outPools := make([]consolePool, 0, len(pools))
	for _, pool := range pools {
		state := "ready"
		if pool.Current < pool.Spec.Min {
			state = "scaling"
		}
		outPools = append(outPools, consolePool{ID: pool.Spec.Name, Vendor: pool.Spec.Vendor, Desired: pool.Spec.Min,
			Actual: pool.Current, Min: pool.Spec.Min, Max: pool.Spec.Max, State: state})
	}
	outWorkspaces := make([]consoleWorkspace, 0, len(workspaces))
	for _, workspace := range workspaces {
		outWorkspaces = append(outWorkspaces, renderConsoleWorkspace(workspace))
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": outNodes, "pools": outPools, "workspaces": outWorkspaces,
		"observedAt": time.Now().UTC().Format(time.RFC3339Nano)})
}

func (s *Server) handleConsoleWorkspaceCreate(w http.ResponseWriter, r *http.Request) {
	cl, _, release := s.consoleClient(w, r, true)
	if cl == nil {
		return
	}
	defer release()
	var input struct {
		Name  string `json:"name"`
		Image string `json:"image,omitempty"`
	}
	if !decodeBody(w, r, &input) {
		return
	}
	if strings.TrimSpace(input.Name) == "" {
		badRequest(w, "workspace name is required")
		return
	}
	workspace, err := cl.CreateWorkspace(r.Context(), proto.WorkspaceSpec{Name: input.Name, Image: input.Image}, idem(r)...)
	if err != nil {
		writeError(w, err)
		return
	}
	detail, err := s.consoleWorkspaceDetails(r.Context(), cl, workspace)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, detail)
}

func (s *Server) handleConsoleWorkspaceGet(w http.ResponseWriter, r *http.Request) {
	cl, _, release := s.consoleClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	workspace, err := cl.GetWorkspace(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	detail, err := s.consoleWorkspaceDetails(r.Context(), cl, workspace)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) consoleWorkspaceDetails(ctx context.Context, cl *client.Client, workspace *proto.Workspace) (*consoleWorkspaceDetail, error) {
	detail := &consoleWorkspaceDetail{consoleWorkspace: renderConsoleWorkspace(*workspace),
		Spec:     consoleWorkspaceSpec{Image: workspace.Spec.Image, Labels: workspace.Spec.Labels, Security: workspace.Spec.Security.Profile},
		Sessions: []consoleSession{}, Snapshots: []consoleSnapshot{}, Generations: []consoleGeneration{}}
	if workspace.State == proto.WSClaimed {
		sessions, err := cl.ListSessions(ctx, workspace.ID)
		if err != nil {
			return nil, err
		}
		for _, session := range sessions {
			row := consoleSession{ID: session.Info.ID, Kind: session.Info.Kind, Command: session.Info.Program,
				StartedAt: millis(session.Info.OpenedAt), Next: session.Next, Earliest: session.Oldest}
			if session.Exited {
				row.EndedAt = millis(workspace.UpdatedAt)
			}
			detail.Sessions = append(detail.Sessions, row)
		}
	}
	events, err := cl.ReadEventPage(ctx, 0, workspace.ID)
	if err != nil {
		var pe *proto.Error
		if !errors.As(err, &pe) || pe.Code != proto.CodeEvicted {
			return nil, err
		}
		first, firstErr := s.Log.First(ctx)
		if firstErr != nil {
			return nil, firstErr
		}
		events, err = cl.ReadEventPage(ctx, first, workspace.ID)
		if err != nil {
			return nil, err
		}
	}
	seenGeneration := map[uint64]bool{}
	seenSnapshot := map[string]bool{}
	for _, event := range events {
		if event.Generation != 0 && !seenGeneration[event.Generation] {
			detail.Generations = append(detail.Generations, consoleGeneration{Generation: event.Generation, Node: event.Node,
				State: event.Type, At: millis(event.At), EventSeq: event.Seq})
			seenGeneration[event.Generation] = true
		}
		if event.Type == proto.EvWSSnapshot {
			payload := decodeConsolePayload(event.Payload)
			artifact, _ := payload["artifact"].(string)
			if artifact == "" {
				artifact, _ = payload["snapshot"].(string)
			}
			if artifact != "" && !seenSnapshot[artifact] {
				detail.Snapshots = append(detail.Snapshots, consoleSnapshot{ID: artifact, Artifact: artifact,
					Generation: event.Generation, CreatedAt: millis(event.At), Bytes: anyInt64(payload["bytes"])})
				seenSnapshot[artifact] = true
			}
		}
	}
	if !seenGeneration[workspace.Generation] {
		detail.Generations = append(detail.Generations, consoleGeneration{Generation: workspace.Generation, Node: workspace.Node,
			State: workspace.State, At: millis(workspace.UpdatedAt)})
	}
	if workspace.LastSnapshot != "" && len(detail.Snapshots) == 0 {
		detail.Snapshots = append(detail.Snapshots, consoleSnapshot{ID: workspace.LastSnapshot, Artifact: workspace.LastSnapshot,
			Generation: workspace.Generation, CreatedAt: millis(workspace.UpdatedAt)})
	}
	return detail, nil
}

func anyInt64(value any) int64 {
	switch n := value.(type) {
	case int64:
		return n
	case uint64:
		if n <= uint64(^uint64(0)>>1) {
			return int64(n)
		}
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}

func (s *Server) handleConsoleWorkspaceAction(w http.ResponseWriter, r *http.Request) {
	cl, _, release := s.consoleClient(w, r, true)
	if cl == nil {
		return
	}
	defer release()
	id := r.PathValue("id")
	key := client.WithIdempotencyKey(r.Header.Get(idempotencyHeader))
	switch r.PathValue("action") {
	case "exec":
		var input struct {
			Command []string `json:"command"`
		}
		if !decodeBody(w, r, &input) {
			return
		}
		if len(input.Command) == 0 {
			badRequest(w, "command is required")
			return
		}
		session, err := cl.Exec(r.Context(), proto.SOpenReq{WS: id, Kind: proto.SessionExec, Program: input.Command,
			NoSubscribe: true, IdempotencyKey: r.Header.Get(idempotencyHeader)})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"session": session.ID})
	case "snapshot":
		if !decodeBody(w, r, &struct{}{}) {
			return
		}
		snapshot, err := cl.Checkpoint(r.Context(), id, key)
		if err != nil {
			writeError(w, err)
			return
		}
		workspace, err := cl.GetWorkspace(r.Context(), id)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"snapshot": snapshot.Artifact, "workspace": renderConsoleWorkspace(*workspace)})
	case "move":
		var input struct {
			Node string `json:"node"`
		}
		if !decodeBody(w, r, &input) {
			return
		}
		if input.Node == "" {
			badRequest(w, "node is required")
			return
		}
		workspace, err := cl.MoveWorkspace(r.Context(), id, nil, &proto.Placement{Node: input.Node}, key)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"workspace": renderConsoleWorkspace(*workspace)})
	case "quarantine":
		if !decodeBody(w, r, &struct{}{}) {
			return
		}
		_, err := cl.QuarantineFleet(r.Context(), proto.FleetQuarantineReq{Selector: proto.WorkspaceSelector{Workspace: id},
			Action: proto.FleetActionFreeze, IdempotencyKey: r.Header.Get(idempotencyHeader)})
		if err != nil {
			writeError(w, err)
			return
		}
		workspace, err := cl.GetWorkspace(r.Context(), id)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"workspace": renderConsoleWorkspace(*workspace)})
	case "destroy":
		if !decodeBody(w, r, &struct{}{}) {
			return
		}
		if err := cl.DestroyWorkspace(r.Context(), id, key); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, proto.Err(proto.CodeNotFound, "no such console workspace action %q", r.PathValue("action")))
	}
}

func (s *Server) handleConsoleFiles(w http.ResponseWriter, r *http.Request) {
	cl, _, release := s.consoleClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	root := r.URL.Query().Get("path")
	if root == "" {
		root = "."
	}
	entries, err := cl.ListDir(r.Context(), r.PathValue("id"), root)
	if err != nil {
		writeError(w, err)
		return
	}
	if len(entries) > consoleCollectionLimit {
		writeError(w, proto.Err(proto.CodeResourceExhausted, "console directory exceeds %d entries", consoleCollectionLimit))
		return
	}
	result := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		kind := "file"
		if entry.IsDir {
			kind = "directory"
		} else if entry.IsLink {
			kind = "symlink"
		}
		result = append(result, map[string]any{"name": entry.Name, "path": pathpkg.Join(root, entry.Name), "kind": kind,
			"size": entry.Size, "modifiedAt": millis(entry.ModTime)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": result})
}

func consoleETag(generation uint64, body []byte) string {
	digest := sha256.Sum256(body)
	return fmt.Sprintf("\"%d-%x\"", generation, digest)
}

func (s *Server) handleConsoleFileRead(w http.ResponseWriter, r *http.Request) {
	cl, _, release := s.consoleClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	workspace, err := cl.GetWorkspace(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	name := r.URL.Query().Get("path")
	data, err := cl.ReadFile(r.Context(), workspace.ID, name)
	if err != nil {
		writeError(w, err)
		return
	}
	if len(data) > consoleFileLimit {
		writeError(w, proto.Err(proto.CodeResourceExhausted, "console text file exceeds %d bytes", consoleFileLimit))
		return
	}
	if !utf8.Valid(data) {
		badRequest(w, "console displays UTF-8 text files only")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"path": name, "content": string(data), "etag": consoleETag(workspace.Generation, data)})
}

func (s *Server) handleConsoleFileWrite(w http.ResponseWriter, r *http.Request) {
	cl, _, release := s.consoleClient(w, r, true)
	if cl == nil {
		return
	}
	defer release()
	name := r.URL.Query().Get("path")
	var input struct {
		Content string `json:"content"`
	}
	if !decodeBody(w, r, &input) {
		return
	}
	if len(input.Content) > consoleFileLimit {
		writeError(w, proto.Err(proto.CodeResourceExhausted, "console text file exceeds %d bytes", consoleFileLimit))
		return
	}
	workspace, err := cl.GetWorkspace(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	current, err := cl.ReadFile(r.Context(), workspace.ID, name)
	if err != nil {
		writeError(w, err)
		return
	}
	if r.Header.Get("If-Match") == "" || r.Header.Get("If-Match") != consoleETag(workspace.Generation, current) {
		writeError(w, proto.Err(proto.CodeConflict, "file or workspace generation changed"))
		return
	}
	data := []byte(input.Content)
	if err := cl.WriteFile(r.Context(), workspace.ID, name, data, 0o644, idem(r)...); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"path": name, "etag": consoleETag(workspace.Generation, data)})
}

func decodeConsolePayload(data []byte) map[string]any {
	if len(data) == 0 {
		return nil
	}
	var payload map[string]any
	if proto.Unmarshal(data, &payload) != nil {
		return map[string]any{"unavailable": true}
	}
	return payload
}

func plainEventType(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, ch := range value {
		if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '.' && ch != '_' && ch != '-' {
			return false
		}
	}
	return true
}

func (s *Server) handleConsoleEvents(w http.ResponseWriter, r *http.Request) {
	cl, subject, release := s.consoleClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	query := r.URL.Query()
	from := uint64(0)
	if value := query.Get("after"); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			badRequest(w, "after must be a non-negative integer")
			return
		}
		from = parsed + 1
	}
	limit := 100
	if value := query.Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 1000 {
			badRequest(w, "limit must be between 1 and 1000")
			return
		}
		limit = parsed
	}
	types := map[string]bool{}
	for _, value := range strings.Split(query.Get("types"), ",") {
		if value == "" {
			continue
		}
		if !plainEventType(value) {
			badRequest(w, "event type %q is invalid", value)
			return
		}
		types[value] = true
	}
	events, err := cl.ReadEventPage(r.Context(), from, "")
	if err != nil {
		writeError(w, err)
		return
	}
	result := make([]consoleEvent, 0, min(limit, len(events)))
	next := from
	for _, event := range events {
		next = event.Seq + 1
		if subject.Tenant != "*" && event.Tenant != "" && event.Tenant != subject.Tenant {
			continue
		}
		if len(types) > 0 && !types[event.Type] || query.Get("principal") != "" && event.Principal != query.Get("principal") ||
			query.Get("session") != "" && event.Session != query.Get("session") {
			continue
		}
		payload := decodeConsolePayload(event.Payload)
		credential, _ := payload["credential"].(string)
		if credential == "" {
			credential, _ = payload["binding"].(string)
		}
		if query.Get("credential") != "" && credential != query.Get("credential") {
			continue
		}
		result = append(result, consoleEvent{Seq: event.Seq, At: millis(event.At), Type: event.Type, Tenant: event.Tenant,
			Workspace: event.Workspace, Session: event.Session, Principal: event.Principal, Credential: credential, Payload: payload})
		if len(result) == limit {
			break
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": result, "next": next})
}

func renderConsoleApproval(approval proto.Approval) consoleApproval {
	status := approval.Status
	if status == proto.ApprovalDecided {
		status = "approved"
		if approval.Decision != nil && approval.Decision.Denied {
			status = "denied"
		}
	}
	kind := approval.Kind
	options := make([]string, 0, len(approval.Options))
	for _, option := range approval.Options {
		options = append(options, option.ID)
	}
	prompt := approval.Title
	if prompt == "" {
		prompt = approval.Host
	}
	return consoleApproval{ID: approval.ID, Agent: approval.Agent, Workspace: approval.WS, Tenant: approval.Tenant,
		Kind: kind, Status: status, Prompt: prompt, Options: options, CreatedAt: millis(approval.CreatedAt)}
}

func (s *Server) handleConsoleApprovals(w http.ResponseWriter, r *http.Request) {
	cl, _, release := s.consoleClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	status := r.URL.Query().Get("status")
	approvals, err := cl.ListApprovals(r.Context(), proto.ApprovalListReq{Status: status})
	if err != nil {
		writeError(w, err)
		return
	}
	if len(approvals) > consoleCollectionLimit {
		writeError(w, proto.Err(proto.CodeResourceExhausted, "console approvals exceed %d rows", consoleCollectionLimit))
		return
	}
	result := make([]consoleApproval, 0, len(approvals))
	for _, approval := range approvals {
		result = append(result, renderConsoleApproval(approval))
	}
	writeJSON(w, http.StatusOK, map[string]any{"approvals": result})
}

type consoleTerminalEvent struct {
	Type    string `json:"type"`
	Session string `json:"session,omitempty"`
	Next    uint64 `json:"next,omitempty"`
	Seq     uint64 `json:"seq,omitempty"`
	Stream  string `json:"stream,omitempty"`
	Data    []byte `json:"data,omitempty"`
	From    uint64 `json:"from,omitempty"`
	To      uint64 `json:"to,omitempty"`
	Code    int    `json:"code,omitempty"`
	Signal  string `json:"signal,omitempty"`
	Error   string `json:"error,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

func (s *Server) handleConsoleTerminal(w http.ResponseWriter, r *http.Request) {
	if !isUpgrade(r) {
		badRequest(w, "the terminal is a WebSocket endpoint")
		return
	}
	cl, _, release := s.consoleClient(w, r, true)
	if cl == nil {
		return
	}
	defer release()
	query := r.URL.Query()
	sessionID := query.Get("session")
	if !plainID(sessionID) {
		badRequest(w, "session is required")
		return
	}
	fromValue, sinceValue := query.Get("from"), query.Get("since")
	if (fromValue == "") == (sinceValue == "") {
		badRequest(w, "exactly one of from or since is required")
		return
	}
	from := uint64(0)
	var since time.Time
	if fromValue != "" {
		parsed, err := strconv.ParseUint(fromValue, 10, 64)
		if err != nil {
			badRequest(w, "from must be a non-negative integer")
			return
		}
		from = parsed
	} else {
		var err error
		since, err = time.Parse(time.RFC3339, sinceValue)
		if err != nil {
			badRequest(w, "since must be RFC3339")
			return
		}
	}
	workspace, err := cl.GetWorkspace(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	// Listing proves the selected session belongs to this workspace without
	// creating or waking a process. The subsequent attach revalidates the
	// generation after the node's workspace-tree boundary is acquired.
	sessions, err := cl.ListSessions(r.Context(), workspace.ID)
	if err != nil {
		writeError(w, err)
		return
	}
	var selected *proto.SessionStatus
	for _, session := range sessions {
		if session.Info.ID == sessionID {
			copy := session
			selected = &copy
			break
		}
	}
	if selected == nil {
		writeError(w, proto.Err(proto.CodeNotFound, "session %q", sessionID))
		return
	}
	if !since.IsZero() {
		switch {
		case since.After(time.Now()):
			from = selected.Next
		default:
			// Session chunks do not carry wall-clock timestamps. Starting at the
			// beginning is conservative: it can replay extra bytes, but an
			// evicted prefix becomes a gap instead of silent omission.
			from = 0
		}
	}
	session, err := cl.Attach(r.Context(), workspace.ID, sessionID, from)
	if err != nil {
		writeError(w, err)
		return
	}
	conn, err := acceptWS(w, r)
	if err != nil {
		_ = session.Close(context.WithoutCancel(r.Context()), false)
		return
	}
	metrics.HTTPTerminalAttaches.Inc()
	s.serveConsoleTerminal(r.Context(), conn, session, from)
}

func (s *Server) serveConsoleTerminal(parent context.Context, conn *websocket.Conn, session *client.Session, from uint64) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
		defer closeCancel()
		_ = session.Close(closeCtx, false)
	}()
	send := func(value consoleTerminalEvent) error {
		writeCtx, writeCancel := context.WithTimeout(ctx, terminalWriteTimeout)
		defer writeCancel()
		body, _ := json.Marshal(value)
		return conn.Write(writeCtx, websocket.MessageText, body)
	}
	if err := send(consoleTerminalEvent{Type: "open", Session: session.ID, Next: from}); err != nil {
		_ = conn.CloseNow()
		return
	}
	go func() {
		defer cancel()
		for {
			typ, body, err := conn.Read(ctx)
			if err != nil {
				return
			}
			if typ == websocket.MessageBinary {
				if session.Input(ctx, body, false) != nil {
					return
				}
				continue
			}
			var command terminalControl
			if json.Unmarshal(body, &command) != nil {
				continue
			}
			var commandErr error
			switch command.Type {
			case "resize":
				if command.Rows > 0 && command.Cols > 0 {
					commandErr = session.Resize(ctx, command.Rows, command.Cols)
				}
			case "signal":
				commandErr = session.Signal(ctx, command.Signal)
			case "input":
				data, err := base64.StdEncoding.DecodeString(command.Data)
				if err == nil {
					commandErr = session.Input(ctx, data, false)
				}
			case "eof":
				commandErr = session.Input(ctx, nil, true)
			}
			if commandErr != nil {
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			_ = conn.Close(websocket.StatusGoingAway, "closed")
			return
		case chunk, ok := <-session.Chunks():
			if !ok {
				if session.Err() != nil {
					_ = conn.Close(websocket.StatusInternalError, "session failed")
				} else {
					_ = conn.Close(websocket.StatusNormalClosure, "session ended")
				}
				return
			}
			switch chunk.Stream {
			case proto.StreamStdout, proto.StreamStderr:
				stream := "stdout"
				if chunk.Stream == proto.StreamStderr {
					stream = "stderr"
				}
				if send(consoleTerminalEvent{Type: "chunk", Seq: chunk.Seq, Stream: stream, Data: chunk.Data}) != nil {
					return
				}
			case proto.StreamGap:
				var gap proto.Gap
				if proto.Unmarshal(chunk.Data, &gap) == nil && send(consoleTerminalEvent{Type: "gap", Seq: chunk.Seq, From: gap.From, To: gap.To}) != nil {
					return
				}
			case proto.StreamExit:
				var exit proto.ExitInfo
				_ = proto.Unmarshal(chunk.Data, &exit)
				_ = send(consoleTerminalEvent{Type: "exit", Seq: chunk.Seq, Code: exit.Code, Signal: exit.Signal, Error: exit.Error, Reason: exit.Reason})
				_ = conn.Close(websocket.StatusNormalClosure, "exit")
				return
			}
		}
	}
}
