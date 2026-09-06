// Package mcp exposes Remount's client surface as Model Context Protocol tools.
package mcp

import (
	"encoding/json"
	"sort"
	"strings"
)

// Tool is the MCP wire description of one tool.
type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	Annotations map[string]any `json:"annotations,omitempty"`
}

// Operation is the single manifest entry from which an operation tool is built.
// ProtocolOp is empty for a composite tool.
type Operation struct {
	Tool       Tool
	ProtocolOp string
}

func objectSchema(properties map[string]any, required ...string) map[string]any {
	s := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func field(kind, description string) map[string]any {
	return map[string]any{"type": kind, "description": description}
}

var composites = []Operation{
	{Tool: Tool{Name: "agent_create", Description: "Create a durable Remount agent. Use parent to create a child with narrowed bindings and policy.", InputSchema: objectSchema(map[string]any{
		"name": field("string", "Optional display name"), "parent": field("string", "Optional parent agent id"),
		"ws": field("string", "Existing workspace id to adopt"), "workspace": field("object", "WorkspaceSpec for a new workspace"),
		"recipe": field("string", "Harness recipe name"), "task": field("string", "Initial task"),
		"bindings": map[string]any{"type": "array", "items": field("string", "Binding ID:provider preset, for example b_openai:openai")},
		"model":    field("string", "Optional model"), "sandbox": field("string", "Harness sandbox profile"),
		"backend": field("string", "Workspace backend"), "image": field("string", "Workspace image"), "security": field("string", "Security profile"),
		"policy": field("object", "AgentPolicy"), "idempotency_key": field("string", "Stable key to reuse for retries"),
	}, "recipe", "task"), Annotations: map[string]any{"destructiveHint": false}}, ProtocolOp: "agent.create"},
	{Tool: Tool{Name: "message", Description: "Send a follow-up or steer message to a durable agent.", InputSchema: objectSchema(map[string]any{
		"id": field("string", "Agent id"), "text": field("string", "Message text"), "kind": field("string", "follow_up or steer"),
		"idempotency_key": field("string", "Stable key to reuse for retries"),
	}, "id", "text")}, ProtocolOp: "agent.message"},
	{Tool: Tool{Name: "get", Description: "Get a durable agent without waking its workspace.", InputSchema: objectSchema(map[string]any{"id": field("string", "Agent id")}, "id"), Annotations: map[string]any{"readOnlyHint": true}}, ProtocolOp: "agent.get"},
	{Tool: Tool{Name: "transcript_tail", Description: "Read one bounded page of a durable agent transcript without waking it; gaps are explicit.", InputSchema: objectSchema(map[string]any{
		"id": field("string", "Agent id"), "from": field("integer", "First transcript index"), "limit": field("integer", "Maximum records, at most 1000"),
	}, "id"), Annotations: map[string]any{"readOnlyHint": true}}, ProtocolOp: "agent.transcript"},
	{Tool: Tool{Name: "approve", Description: "Decide one pending approval. The decision is durable and idempotent.", InputSchema: objectSchema(map[string]any{
		"id": field("string", "Approval id"), "option": field("string", "Offered option id"), "denied": field("boolean", "Reject without selecting an option"),
		"content": field("object", "Elicitation response object"), "remember": field("string", "none, host, or rule for egress approvals"),
		"idempotency_key": field("string", "Stable key to reuse for retries"),
	}, "id"), Annotations: map[string]any{"destructiveHint": true}}, ProtocolOp: "approval.decide"},
	{Tool: Tool{Name: "run", Description: "Run an argv in a claimed workspace and return bounded stdout, stderr, and exit status.", InputSchema: objectSchema(map[string]any{
		"ws": field("string", "Workspace id"), "program": map[string]any{"type": "array", "items": field("string", "argv element"), "minItems": 1},
	}, "ws", "program")}},
	{Tool: Tool{Name: "handoff", Description: "Move a workspace to new placement requirements. The source remains fenced until the durable checkpoint commits.", InputSchema: objectSchema(map[string]any{
		"ws": field("string", "Workspace id"), "requires": field("object", "Optional Requires replacement"), "placement": field("object", "Optional Placement replacement"),
		"idempotency_key": field("string", "Stable key to reuse for retries"),
	}, "ws"), Annotations: map[string]any{"destructiveHint": true}}},
	{Tool: Tool{Name: "resume", Description: "Wake a paused workspace and return its current durable state.", InputSchema: objectSchema(map[string]any{
		"ws": field("string", "Workspace id"), "idempotency_key": field("string", "Stable key to reuse for retries"),
	}, "ws")}},
	{Tool: Tool{Name: "events_tail", Description: "Read one bounded page of canonical events from a sequence. This call does not keep an unbounded stream open.", InputSchema: objectSchema(map[string]any{
		"from": field("integer", "First event sequence"), "ws": field("string", "Optional workspace filter"),
	}, "from"), Annotations: map[string]any{"readOnlyHint": true}}, ProtocolOp: "events.tail"},
}

// protocolOperations is the complete generated protocol-op manifest. Each
// entry becomes op_<name>, with dots changed to underscores. Node/control
// internal operations are deliberately present but answer unavailable when
// invoked through a client-role MCP server.
var protocolOperations = []string{
	"agent.approval.decided", "agent.cancel", "agent.create", "agent.deliver", "agent.destroy", "agent.fork", "agent.get", "agent.list", "agent.message", "agent.report", "agent.run", "agent.run.cancel", "agent.sleep", "agent.transcript", "agent.wake",
	"approval.decide", "approval.get", "approval.list", "artifact.proof", "audit.export", "audit.key", "base.create", "base.list", "base.remove", "binding.lease",
	"binding.create", "binding.get", "binding.list", "binding.revoke", "binding.rotate",
	"budget.create", "budget.list", "budget.remove", "budget.reserve", "budget.settle",
	"computer.close", "computer.create", "computer.downloads", "computer.eval", "computer.get", "computer.input", "computer.navigate", "computer.screenshot",
	"controller.state", "diag", "egress.approval",
	"events.post", "events.stop", "events.tail", "fleet.get", "fleet.list", "fleet.quarantine",
	"fs.apply_tar", "fs.edit", "fs.list", "fs.mkdir", "fs.read", "fs.remove", "fs.rename", "fs.search", "fs.stat", "fs.write", "grant",
	"node.diag", "node.enroll", "node.list", "node.profile.get", "node.status", "pool.create", "pool.get", "pool.list", "pool.remove", "port.open", "principal.create", "principal.invite", "principal.list", "principal.revoke", "principal.session.create", "principal.token.issue",
	"queue.advance", "queue.create", "queue.get", "queue.list", "s.ack", "s.attach", "s.close", "s.input", "s.list", "s.open", "s.resize", "s.signal", "s.wait",
	"session.cap.check", "session.cap.issue", "session.cap.renew", "session.log.commit", "session.log.delete", "session.log.get", "tenant.create", "tenant.get", "tenant.list", "tenant.state", "tenant.update", "tenant.usage",
	"timer.list", "usage.get", "volume.archive", "volume.attach", "volume.create", "volume.detach", "volume.get", "volume.list", "volume.publish", "volume.publish.commit", "volume.remove", "ws.acl", "ws.claim", "ws.create", "ws.destroy", "ws.get", "ws.idle.mark", "ws.idle.policy", "ws.info", "ws.lease", "ws.lease.cancel", "ws.lease.get", "ws.lease.renew", "ws.list", "ws.move", "ws.quarantine", "ws.quarantine.commit", "ws.ready", "ws.release", "ws.release.abort", "ws.release.abort.commit", "ws.release.commit", "ws.released", "ws.renew", "ws.sleep", "ws.snapshot", "ws.snapshot.commit", "ws.wake",
}

// protocolToolName is the tool name advertised for a protocol operation.
func protocolToolName(op string) string {
	return "op_" + strings.ReplaceAll(op, ".", "_")
}

// protocolOpByTool resolves an advertised tool name back to its exact protocol
// operation. The forward mapping replaces dots with underscores, so reversing
// it by replacing underscores with dots is lossy for any op that already
// contains an underscore: `fs.apply_tar` becomes `op_fs_apply_tar` and reverses
// to `fs.apply.tar`, an operation that does not exist. That advertised the tool
// while making it permanently unreachable, so the reverse direction is a lookup
// over the manifest rather than a second string substitution.
var protocolOpByTool = func() map[string]string {
	out := make(map[string]string, len(protocolOperations))
	for _, op := range protocolOperations {
		out[protocolToolName(op)] = op
	}
	return out
}()

// ProtocolOpForTool returns the protocol operation an op_ tool name names, and
// whether the manifest advertises it.
func ProtocolOpForTool(name string) (string, bool) {
	op, ok := protocolOpByTool[name]
	return op, ok
}

// Manifest returns a stable copy of every composite and protocol operation.
func Manifest() []Operation {
	out := make([]Operation, 0, len(composites)+len(protocolOperations))
	out = append(out, composites...)
	for _, op := range protocolOperations {
		name := protocolToolName(op)
		out = append(out, Operation{Tool: Tool{
			Name:        name,
			Description: "Invoke the " + op + " Remount protocol operation with its JSON request body. Client-inaccessible peer operations report unavailable.",
			InputSchema: map[string]any{"type": "object", "additionalProperties": true},
		}, ProtocolOp: op})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Tool.Name < out[j].Tool.Name })
	return cloneManifest(out)
}

func cloneManifest(in []Operation) []Operation {
	raw, _ := json.Marshal(in)
	var out []Operation
	_ = json.Unmarshal(raw, &out)
	return out
}
