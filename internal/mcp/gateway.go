package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/proto"
)

// Gateway performs an MCP tool's Remount operation. Implementations must not
// retain arguments after Invoke returns.
type Gateway interface {
	Invoke(context.Context, string, json.RawMessage) (any, error)
}

// UnavailableError means a manifest tool cannot honestly be performed from a
// client-role connection. The tool remains discoverable instead of pretending
// an internal peer operation succeeded.
type UnavailableError struct{ Operation string }

func (e *UnavailableError) Error() string {
	return "operation " + e.Operation + " is unavailable from an MCP client"
}

// ClientGateway adapts the reconnecting Go client to MCP calls.
type ClientGateway struct{ Client *client.Client }

func decode[T any](raw json.RawMessage) (T, error) {
	var value T
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	if err := dec.Decode(&value); err != nil {
		return value, proto.Err(proto.CodeBadRequest, "invalid tool arguments: %v", err)
	}
	return value, nil
}

func operationOption(key string) []client.OperationOption {
	if key == "" {
		return nil
	}
	return []client.OperationOption{client.WithIdempotencyKey(key)}
}

// Invoke executes a composite or a client-accessible protocol operation.
func (g *ClientGateway) Invoke(ctx context.Context, name string, raw json.RawMessage) (any, error) {
	if g == nil || g.Client == nil {
		return nil, errors.New("remount client is unavailable")
	}
	switch name {
	case "agent_create":
		var in struct {
			Name           string               `json:"name"`
			Parent         string               `json:"parent"`
			WS             string               `json:"ws"`
			Recipe         string               `json:"recipe"`
			Task           string               `json:"task"`
			Model          string               `json:"model"`
			Sandbox        string               `json:"sandbox"`
			Backend        string               `json:"backend"`
			Image          string               `json:"image"`
			Security       string               `json:"security"`
			IdempotencyKey string               `json:"idempotency_key"`
			Workspace      *proto.WorkspaceSpec `json:"workspace"`
			Bindings       []string             `json:"bindings"`
			Policy         proto.AgentPolicy    `json:"policy"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "invalid tool arguments: %v", err)
		}
		req := proto.AgentCreateReq{Name: in.Name, Parent: in.Parent, WS: in.WS, Workspace: in.Workspace, Policy: in.Policy,
			Spec: proto.AgentSpec{Recipe: in.Recipe, Task: in.Task, Model: in.Model, Sandbox: in.Sandbox, Mode: proto.AgentModeACP}, IdempotencyKey: in.IdempotencyKey}
		if in.WS == "" && in.Workspace == nil && in.Parent != "" {
			// The control plane inherits bindings, security, providers and
			// policy caps from the parent before committing the child.
			req.Workspace = &proto.WorkspaceSpec{}
		}
		if in.WS == "" && in.Workspace == nil && in.Parent == "" {
			recipe, err := launch.Load(in.Recipe)
			if err != nil {
				return nil, proto.Err(proto.CodeBadRequest, "recipe %q is unavailable", in.Recipe)
			}
			opts := launch.Options{Recipe: recipe, Task: in.Task, Name: in.Name, Model: in.Model, Sandbox: in.Sandbox,
				Backend: in.Backend, Image: in.Image, Security: in.Security, Approve: proto.ApproveNever}
			for _, spec := range in.Bindings {
				// Parse without echoing a potentially malformed value: binding
				// identifiers are authorization inputs and do not belong in logs.
				binding, err := launch.ParseBinding(spec)
				if err != nil {
					return nil, proto.Err(proto.CodeBadRequest, "invalid binding specification")
				}
				opts.Bindings = append(opts.Bindings, binding)
			}
			plan, err := opts.Validate()
			if err != nil {
				return nil, err
			}
			req.Workspace = &plan.Spec
			req.Spec.Providers, req.Spec.Primary, req.Spec.Auth = plan.Data.Providers, plan.Data.Primary, plan.Auth
			if req.Spec.Sandbox == "" {
				req.Spec.Sandbox = plan.Data.Sandbox
			}
		}
		return g.Client.CreateAgent(ctx, req, operationOption(in.IdempotencyKey)...)
	case "message":
		var in struct {
			ID             string `json:"id"`
			Text           string `json:"text"`
			Kind           string `json:"kind"`
			IdempotencyKey string `json:"idempotency_key"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "invalid tool arguments: %v", err)
		}
		return g.Client.MessageAgent(ctx, proto.AgentMessageReq{ID: in.ID, Text: in.Text, Kind: in.Kind, IdempotencyKey: in.IdempotencyKey}, operationOption(in.IdempotencyKey)...)
	case "get":
		in, err := decode[proto.AgentGetReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.GetAgent(ctx, in.ID)
	case "transcript_tail":
		in, err := decode[proto.AgentTranscriptReq](raw)
		if err != nil {
			return nil, err
		}
		if in.Limit < 0 || in.Limit > proto.MaxTranscriptPage {
			return nil, proto.Err(proto.CodeBadRequest, "limit must be between 0 and %d", proto.MaxTranscriptPage)
		}
		return g.Client.Transcript(ctx, in.ID, in.From, in.Limit)
	case "approve":
		var req proto.ApprovalDecideReq
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "invalid tool arguments: %v", err)
		}
		return g.Client.DecideApproval(ctx, req, operationOption(req.IdempotencyKey)...)
	case "run":
		var in struct {
			WS      string   `json:"ws"`
			Program []string `json:"program"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "invalid tool arguments: %v", err)
		}
		if in.WS == "" || len(in.Program) == 0 {
			return nil, proto.Err(proto.CodeBadRequest, "ws and a non-empty program are required")
		}
		for _, arg := range in.Program {
			if strings.Contains(strings.ReplaceAll(arg, "\\", "/"), ".remount/env") {
				return nil, proto.Err(proto.CodeDenied, ".remount/env is not readable through MCP")
			}
		}
		stdout, stderr, exit, err := g.Client.Run(ctx, in.WS, in.Program...)
		if err != nil {
			return nil, err
		}
		return map[string]any{"stdout": string(stdout), "stderr": string(stderr), "exit": exit}, nil
	case "handoff":
		var in struct {
			WS             string           `json:"ws"`
			Requires       *proto.Requires  `json:"requires"`
			Placement      *proto.Placement `json:"placement"`
			IdempotencyKey string           `json:"idempotency_key"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "invalid tool arguments: %v", err)
		}
		return g.Client.MoveWorkspace(ctx, in.WS, in.Requires, in.Placement, operationOption(in.IdempotencyKey)...)
	case "resume":
		var in struct {
			WS             string `json:"ws"`
			IdempotencyKey string `json:"idempotency_key"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "invalid tool arguments: %v", err)
		}
		ws, err := g.Client.GetWorkspace(ctx, in.WS)
		if err != nil {
			return nil, err
		}
		if ws.State == proto.WSPaused || ws.State == proto.WSReleased || ws.State == proto.WSPending {
			return g.Client.WakeWorkspace(ctx, in.WS, operationOption(in.IdempotencyKey)...)
		}
		return ws, nil
	case "events_tail":
		in, err := decode[proto.EventsTailReq](raw)
		if err != nil {
			return nil, err
		}
		events, err := g.Client.ReadEventPage(ctx, in.From, in.WS)
		if err != nil {
			return nil, err
		}
		next := in.From
		if len(events) > 0 {
			next = events[len(events)-1].Seq + 1
		}
		return map[string]any{"events": events, "next": next}, nil
	}
	if op, ok := ProtocolOpForTool(name); ok {
		return g.invokeProtocol(ctx, op, raw)
	}
	return nil, &UnavailableError{Operation: name}
}

func (g *ClientGateway) invokeProtocol(ctx context.Context, op string, raw json.RawMessage) (any, error) {
	switch op {
	case proto.OpAgentCreate:
		req, err := decode[proto.AgentCreateReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.CreateAgent(ctx, req, operationOption(req.IdempotencyKey)...)
	case proto.OpAgentGet:
		req, err := decode[proto.AgentGetReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.GetAgent(ctx, req.ID)
	case proto.OpAgentList:
		req, err := decode[proto.AgentListReq](raw)
		if err != nil {
			return nil, err
		}
		items, err := g.Client.ListAgents(ctx, req)
		return proto.AgentListRes{Agents: items}, err
	case proto.OpAgentMessage:
		req, err := decode[proto.AgentMessageReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.MessageAgent(ctx, req, operationOption(req.IdempotencyKey)...)
	case proto.OpAgentCancel:
		req, err := decode[proto.AgentGetReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.CancelAgent(ctx, req.ID, operationOption(req.IdempotencyKey)...)
	case proto.OpAgentSleep:
		req, err := decode[proto.AgentGetReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.SleepAgent(ctx, req.ID, operationOption(req.IdempotencyKey)...)
	case proto.OpAgentWake:
		req, err := decode[proto.AgentWakeReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.WakeAgent(ctx, req.ID, req.By, operationOption(req.IdempotencyKey)...)
	case proto.OpAgentFork:
		req, err := decode[proto.AgentForkReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.ForkAgent(ctx, req, operationOption(req.IdempotencyKey)...)
	case proto.OpAgentDestroy:
		req, err := decode[proto.AgentGetReq](raw)
		if err != nil {
			return nil, err
		}
		return emptyResult(g.Client.DestroyAgent(ctx, req.ID, operationOption(req.IdempotencyKey)...))
	case proto.OpAgentTranscript:
		req, err := decode[proto.AgentTranscriptReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.Transcript(ctx, req.ID, req.From, req.Limit)
	case proto.OpApprovalList:
		req, err := decode[proto.ApprovalListReq](raw)
		if err != nil {
			return nil, err
		}
		items, err := g.Client.ListApprovals(ctx, req)
		return proto.ApprovalListRes{Approvals: items}, err
	case proto.OpApprovalGet:
		req, err := decode[proto.ApprovalGetReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.GetApproval(ctx, req.ID)
	case proto.OpApprovalDecide:
		req, err := decode[proto.ApprovalDecideReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.DecideApproval(ctx, req, operationOption(req.IdempotencyKey)...)
	case proto.OpWSCreate:
		req, err := decode[proto.WSCreateReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.CreateWorkspace(ctx, req.Spec, operationOption(req.IdempotencyKey)...)
	case proto.OpWSGet:
		req, err := decode[proto.WSGetReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.GetWorkspace(ctx, req.ID)
	case proto.OpWSList:
		items, err := g.Client.ListWorkspaces(ctx)
		return proto.WSListRes{Workspaces: items}, err
	case proto.OpWSDestroy:
		req, err := decode[proto.WSGetReq](raw)
		if err != nil {
			return nil, err
		}
		return emptyResult(g.Client.DestroyWorkspace(ctx, req.ID, operationOption(req.IdempotencyKey)...))
	case proto.OpWSMove:
		req, err := decode[proto.WSMoveReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.MoveWorkspace(ctx, req.ID, req.Requires, req.Placement, operationOption(req.IdempotencyKey)...)
	case proto.OpWSSleep:
		req, err := decode[proto.WSSleepReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.SleepWorkspace(ctx, req, operationOption(req.IdempotencyKey)...)
	case proto.OpWSWake:
		req, err := decode[proto.WSGetReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.WakeWorkspace(ctx, req.ID, operationOption(req.IdempotencyKey)...)
	case proto.OpWSACL:
		req, err := decode[proto.WSACLReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.SetWorkspaceACL(ctx, req.ID, req.ACL, operationOption(req.IdempotencyKey)...)
	case proto.OpNodeList:
		items, err := g.Client.ListNodes(ctx)
		return proto.NodeListRes{Nodes: items}, err
	case proto.OpTimerList:
		items, err := g.Client.ListTimers(ctx)
		return proto.TimerListRes{Timers: items}, err
	case proto.OpDiag:
		req, err := decode[proto.DiagReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.Diag(ctx, req.Verify)
	case proto.OpEventsPost:
		req, err := decode[proto.EventPost](raw)
		if err != nil {
			return nil, err
		}
		for _, event := range req.Events {
			if err := g.Client.PostEvent(ctx, event); err != nil {
				return nil, err
			}
		}
		return map[string]any{}, nil
	case proto.OpBaseCreate:
		req, err := decode[proto.BaseCreateReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.CreateBase(ctx, req, operationOption(req.IdempotencyKey)...)
	case proto.OpBaseList:
		items, err := g.Client.ListBases(ctx)
		return proto.BaseListRes{Bases: items}, err
	case proto.OpBaseRemove:
		req, err := decode[proto.BaseRemoveReq](raw)
		if err != nil {
			return nil, err
		}
		return emptyResult(g.Client.RemoveBase(ctx, req.Name, operationOption(req.IdempotencyKey)...))
	case proto.OpVolumeCreate:
		req, err := decode[proto.VolumeCreateReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.CreateVolume(ctx, req, operationOption(req.IdempotencyKey)...)
	case proto.OpVolumeGet:
		req, err := decode[proto.VolumeGetReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.GetVolume(ctx, req.ID)
	case proto.OpVolumeList:
		items, err := g.Client.ListVolumes(ctx)
		return proto.VolumeListRes{Volumes: items}, err
	case proto.OpVolumeRemove:
		req, err := decode[proto.VolumeRemoveReq](raw)
		if err != nil {
			return nil, err
		}
		return emptyResult(g.Client.RemoveVolume(ctx, req.ID, operationOption(req.IdempotencyKey)...))
	case proto.OpVolumePublish:
		req, err := decode[proto.VolumePublishPathReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.PublishVolumePath(ctx, req.WS, req.Path, req.Volume, req.ExpectedVersion, operationOption(req.IdempotencyKey)...)
	case proto.OpVolumeAttach:
		req, err := decode[proto.VolumeAttachReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.AttachVolume(ctx, req, operationOption(req.IdempotencyKey)...)
	case proto.OpVolumeDetach:
		req, err := decode[proto.VolumeDetachReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.DetachVolume(ctx, req, operationOption(req.IdempotencyKey)...)
	case proto.OpVolumeArchive:
		req, err := decode[proto.VolumeArchiveReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.ArchiveVolumeWithUpload(ctx, req.WS, req.Path, req.Upload, operationOption(req.IdempotencyKey)...)
	case proto.OpPoolCreate:
		req, err := decode[proto.PoolCreateReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.CreatePool(ctx, req.Spec, operationOption(req.IdempotencyKey)...)
	case proto.OpPoolGet:
		req, err := decode[proto.PoolGetReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.GetPool(ctx, req.Name)
	case proto.OpPoolList:
		items, err := g.Client.ListPools(ctx)
		return proto.PoolListRes{Pools: items}, err
	case proto.OpPoolRemove:
		req, err := decode[proto.PoolRemoveReq](raw)
		if err != nil {
			return nil, err
		}
		return emptyResult(g.Client.RemovePool(ctx, req.Name, operationOption(req.IdempotencyKey)...))
	case proto.OpQueueCreate:
		req, err := decode[proto.QueueCreateReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.CreateQueue(ctx, req, operationOption(req.IdempotencyKey)...)
	case proto.OpQueueGet:
		req, err := decode[proto.QueueGetReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.GetQueue(ctx, req.ID)
	case proto.OpQueueList:
		req, err := decode[proto.QueueListReq](raw)
		if err != nil {
			return nil, err
		}
		items, err := g.Client.ListQueues(ctx, req.WS)
		return proto.QueueListRes{Queues: items}, err
	case proto.OpQueueAdvance:
		req, err := decode[proto.QueueAdvanceReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.AdvanceQueue(ctx, req, operationOption(req.IdempotencyKey)...)
	case proto.OpFleetQuarantine:
		req, err := decode[proto.FleetQuarantineReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.QuarantineFleet(ctx, req)
	case proto.OpFleetGet:
		req, err := decode[proto.FleetGetReq](raw)
		if err != nil {
			return nil, err
		}
		return g.Client.GetFleetOperation(ctx, req.ID)
	case proto.OpFleetList:
		items, err := g.Client.ListFleetOperations(ctx)
		return proto.FleetListRes{Operations: items}, err
	case proto.OpFSList:
		req, err := decode[proto.FSListReq](raw)
		if err != nil {
			return nil, err
		}
		items, err := g.Client.ListDir(ctx, req.WS, req.Path)
		return proto.FSListRes{Entries: items}, err
	case proto.OpFSStat:
		req, err := decode[proto.FSListReq](raw)
		if err != nil {
			return nil, err
		}
		item, err := g.Client.Stat(ctx, req.WS, req.Path)
		if err != nil {
			return nil, err
		}
		return proto.FSStatRes{Entry: *item}, nil
	case proto.OpFSMkdir:
		req, err := decode[proto.FSMkdirReq](raw)
		if err != nil {
			return nil, err
		}
		return emptyResult(g.Client.Mkdir(ctx, req.WS, req.Path, operationOption(req.IdempotencyKey)...))
	case proto.OpFSRemove:
		req, err := decode[proto.FSRemoveReq](raw)
		if err != nil {
			return nil, err
		}
		return emptyResult(g.Client.Remove(ctx, req.WS, req.Path, req.Recursive, operationOption(req.IdempotencyKey)...))
	case proto.OpFSRename:
		req, err := decode[proto.FSRenameReq](raw)
		if err != nil {
			return nil, err
		}
		return emptyResult(g.Client.Rename(ctx, req.WS, req.From, req.To, operationOption(req.IdempotencyKey)...))
	case proto.OpFSSearch:
		req, err := decode[proto.FSSearchReq](raw)
		if err != nil {
			return nil, err
		}
		if protectedPath(req.Path) {
			return nil, proto.Err(proto.CodeDenied, ".remount/env is not readable through MCP")
		}
		return g.Client.Search(ctx, req.WS, req.Path, req.Pattern, req.Glob, req.MaxResults)
	case proto.OpFSEdit:
		req, err := decode[proto.FSEditReq](raw)
		if err != nil {
			return nil, err
		}
		if protectedPath(req.Path) {
			return nil, proto.Err(proto.CodeDenied, ".remount/env is not writable through MCP")
		}
		n, err := g.Client.Edit(ctx, req.WS, req.Path, req.Edits, operationOption(req.IdempotencyKey)...)
		return proto.FSEditRes{Replacements: n}, err
	case proto.OpFSApplyTar:
		req, err := decode[proto.FSApplyTarReq](raw)
		if err != nil {
			return nil, err
		}
		// Carry the requested representation through. Dropping it would apply a
		// chunked manifest as though it were a tar archive: the node fails
		// closed on the header rather than corrupting the tree, but the
		// operation would be unusable through MCP for any non-tar format.
		return g.Client.ApplyArtifact(ctx, req.WS, req.Artifact, req.Format, operationOption(req.IdempotencyKey)...)
	case proto.OpWSSnapshot:
		req, err := decode[proto.WSSnapshotReq](raw)
		if err != nil {
			return nil, err
		}
		if req.Authoritative {
			return g.Client.Checkpoint(ctx, req.WS, operationOption(req.IdempotencyKey)...)
		}
		return g.Client.Snapshot(ctx, req.WS, req.Upload, operationOption(req.IdempotencyKey)...)
	case proto.OpWSInfo:
		req, err := decode[proto.WSGetReq](raw)
		if err != nil {
			return nil, err
		}
		info, err := g.Client.WorkspaceInfo(ctx, req.ID)
		if err != nil {
			return nil, err
		}
		return map[string]any{"ws": info.WS, "broker": info.Broker}, nil
	case proto.OpSList:
		req, err := decode[proto.SListReq](raw)
		if err != nil {
			return nil, err
		}
		items, err := g.Client.ListSessions(ctx, req.WS)
		return proto.SListRes{Sessions: items}, err
	case proto.OpNodeDiag:
		if _, err := decode[proto.NodeDiagReq](raw); err != nil {
			return nil, err
		}
		return nil, &UnavailableError{Operation: fmt.Sprintf("%s (use the typed doctor surface with an explicit node id)", op)}
	case proto.OpNodeStatus:
		return nil, &UnavailableError{Operation: fmt.Sprintf("%s (request has no node id in the wire contract)", op)}
	default:
		return nil, &UnavailableError{Operation: op}
	}
}

func emptyResult(err error) (any, error) {
	if err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}

func protectedPath(p string) bool {
	p = strings.TrimPrefix(path.Clean(strings.ReplaceAll(p, "\\", "/")), "/")
	p = strings.TrimPrefix(p, "./")
	return p == ".remount/env" || strings.HasSuffix(p, "/.remount/env")
}
