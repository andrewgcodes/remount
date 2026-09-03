// Package client is the supported Go SDK for Remount. A Client reconnects
// transparently, retries with bounded policy, refreshes workspace grants, and
// resumes attached session/event streams from durable cursors.
//
// Mutating methods accept WithIdempotencyKey where their request does not
// already carry an IdempotencyKey field. Reuse the same key when recovering an
// ambiguous outcome after a timeout or process restart.
package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"remount.dev/remount/api"
	internalclient "remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

// Options configures a Client.
type Options struct {
	// Server is an http(s) Remount base URL or a ws(s) link URL. It defaults
	// to http://127.0.0.1:7443.
	Server string
	// Token is sent inside the authenticated hello frame, never in the URL.
	Token string
	// Principal is an audit hint only. The server derives authority from the
	// authenticated token and ignores client-selected identity for access.
	Principal string
	// Headers are copied onto the WebSocket upgrade request.
	Headers http.Header
	// Retries bounds connection-loss retries per call. Zero uses five.
	Retries int
	// MaxReadBytes bounds ReadFile's aggregate allocation. Zero uses 64 MiB.
	MaxReadBytes int64
	// MaxRunOutputBytes bounds stdout+stderr collected by Run. Zero uses 64
	// MiB; use Exec and Session.Chunks for larger streaming output.
	MaxRunOutputBytes int64
}

// OperationOption configures one logical mutating operation.
type OperationOption = internalclient.OperationOption

// WithIdempotencyKey supplies a caller-owned key that remains stable across
// retries and client restarts. Reusing it with different arguments returns a
// conflict instead of replaying the wrong result.
func WithIdempotencyKey(key string) OperationOption {
	return internalclient.WithIdempotencyKey(key)
}

// Client owns one reconnecting logical connection and all streams opened
// through it. Close is idempotent and wakes those streams.
type Client struct {
	inner *internalclient.Client
}

// New validates configuration and constructs a lazily connecting client.
func New(options Options) (*Client, error) {
	if options.Retries < 0 {
		return nil, fmt.Errorf("client: retries cannot be negative")
	}
	if options.MaxReadBytes < 0 {
		return nil, fmt.Errorf("client: max read bytes cannot be negative")
	}
	if options.MaxRunOutputBytes < 0 {
		return nil, fmt.Errorf("client: max run output bytes cannot be negative")
	}
	link, err := normalizeLinkURL(options.Server)
	if err != nil {
		return nil, err
	}
	headers := options.Headers.Clone()
	dialer := transport.DialFunc(func(ctx context.Context) (transport.Conn, error) {
		return transport.DialWS(ctx, link, headers.Clone())
	})
	return &Client{inner: internalclient.New(internalclient.Options{
		Dialer: dialer, Token: options.Token, Principal: options.Principal,
		Retries: options.Retries, MaxReadBytes: options.MaxReadBytes,
		MaxRunOutputBytes: options.MaxRunOutputBytes,
	})}, nil
}

func normalizeLinkURL(server string) (string, error) {
	if server == "" {
		server = "http://127.0.0.1:7443"
	}
	u, err := url.Parse(server)
	if err != nil {
		return "", fmt.Errorf("client: parse server URL: %w", err)
	}
	if u.User != nil || u.Host == "" || u.Fragment != "" {
		return "", fmt.Errorf("client: server URL must have a host and no userinfo or fragment")
	}
	switch strings.ToLower(u.Scheme) {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("client: unsupported server URL scheme %q", u.Scheme)
	}
	path := strings.TrimSuffix(u.EscapedPath(), "/")
	if path == "" {
		path = "/v1/link"
	} else if path != "/v1/link" && !strings.HasSuffix(path, "/v1/link") {
		path += "/v1/link"
	}
	unescaped, err := url.PathUnescape(path)
	if err != nil {
		return "", fmt.Errorf("client: invalid server URL path: %w", err)
	}
	u.Path, u.RawPath = unescaped, path
	return u.String(), nil
}

// ID is the current relay-assigned peer id, or empty before first connect.
func (c *Client) ID() string { return c.inner.ID() }

// Close disconnects and wakes all owned streams. It is safe to call twice.
func (c *Client) Close() error { return c.inner.Close() }

func (c *Client) CreateWorkspace(ctx context.Context, spec api.WorkspaceSpec, options ...OperationOption) (*api.Workspace, error) {
	return c.inner.CreateWorkspace(ctx, spec, options...)
}

func (c *Client) GetWorkspace(ctx context.Context, id string) (*api.Workspace, error) {
	return c.inner.GetWorkspace(ctx, id)
}

func (c *Client) WaitClaimed(ctx context.Context, id string) (*api.Workspace, error) {
	return c.inner.WaitClaimed(ctx, id)
}

func (c *Client) ListWorkspaces(ctx context.Context) ([]api.Workspace, error) {
	return c.inner.ListWorkspaces(ctx)
}

func (c *Client) DestroyWorkspace(ctx context.Context, id string, options ...OperationOption) error {
	return c.inner.DestroyWorkspace(ctx, id, options...)
}

func (c *Client) MoveWorkspace(ctx context.Context, id string, requirements *api.Requires, placement *api.Placement, options ...OperationOption) (*api.Workspace, error) {
	return c.inner.MoveWorkspace(ctx, id, requirements, placement, options...)
}

func (c *Client) SleepWorkspace(ctx context.Context, request api.SleepRequest, options ...OperationOption) (*api.Timer, error) {
	return c.inner.SleepWorkspace(ctx, request, options...)
}

func (c *Client) WakeWorkspace(ctx context.Context, id string, options ...OperationOption) (*api.Workspace, error) {
	return c.inner.WakeWorkspace(ctx, id, options...)
}

func (c *Client) ListNodes(ctx context.Context) ([]api.NodeStatus, error) {
	return c.inner.ListNodes(ctx)
}

func (c *Client) QuarantineFleet(ctx context.Context, request api.FleetQuarantineRequest) (*api.FleetOperation, error) {
	return c.inner.QuarantineFleet(ctx, request)
}

func (c *Client) GetFleetOperation(ctx context.Context, id string) (*api.FleetOperation, error) {
	return c.inner.GetFleetOperation(ctx, id)
}

func (c *Client) ListFleetOperations(ctx context.Context) ([]api.FleetOperation, error) {
	return c.inner.ListFleetOperations(ctx)
}

func (c *Client) WaitFleetOperation(ctx context.Context, id string) (*api.FleetOperation, error) {
	return c.inner.WaitFleetOperation(ctx, id)
}

func (c *Client) ListTimers(ctx context.Context) ([]api.Timer, error) {
	return c.inner.ListTimers(ctx)
}

func (c *Client) PostEvent(ctx context.Context, event api.Event) error {
	return c.inner.PostEvent(ctx, event)
}

func (c *Client) ReadEvents(ctx context.Context, from uint64, workspace string) ([]api.Event, error) {
	return c.inner.ReadEvents(ctx, from, workspace)
}

// TailEvents returns an ordered replayable stream. Cancel ctx or close Client
// to release both the local and remote subscription.
func (c *Client) TailEvents(ctx context.Context, from uint64, workspace string) (<-chan api.Event, error) {
	return c.inner.TailEvents(ctx, from, workspace)
}

func (c *Client) ReadFile(ctx context.Context, workspace, path string) ([]byte, error) {
	return c.inner.ReadFile(ctx, workspace, path)
}

func (c *Client) WriteFile(ctx context.Context, workspace, path string, data []byte, mode uint32, options ...OperationOption) error {
	return c.inner.WriteFile(ctx, workspace, path, data, mode, options...)
}

func (c *Client) ListDir(ctx context.Context, workspace, path string) ([]api.FileEntry, error) {
	return c.inner.ListDir(ctx, workspace, path)
}

func (c *Client) Stat(ctx context.Context, workspace, path string) (*api.FileEntry, error) {
	return c.inner.Stat(ctx, workspace, path)
}

func (c *Client) Mkdir(ctx context.Context, workspace, path string, options ...OperationOption) error {
	return c.inner.Mkdir(ctx, workspace, path, options...)
}

func (c *Client) Remove(ctx context.Context, workspace, path string, recursive bool, options ...OperationOption) error {
	return c.inner.Remove(ctx, workspace, path, recursive, options...)
}

func (c *Client) Rename(ctx context.Context, workspace, from, to string, options ...OperationOption) error {
	return c.inner.Rename(ctx, workspace, from, to, options...)
}

func (c *Client) Search(ctx context.Context, workspace, path, pattern, glob string, max int) (*api.FileSearchResult, error) {
	return c.inner.Search(ctx, workspace, path, pattern, glob, max)
}

func (c *Client) Edit(ctx context.Context, workspace, path string, edits []api.FileEdit, options ...OperationOption) (int, error) {
	return c.inner.Edit(ctx, workspace, path, edits, options...)
}

func (c *Client) Snapshot(ctx context.Context, workspace string, upload bool, options ...OperationOption) (*api.SnapshotResult, error) {
	return c.inner.Snapshot(ctx, workspace, upload, options...)
}

// Checkpoint quiesces managed execution, uploads the archive, and commits it
// as authoritative failover state. The local process backend terminates the
// workspace's sessions to meet that contract.
func (c *Client) Checkpoint(ctx context.Context, workspace string, options ...OperationOption) (*api.SnapshotResult, error) {
	return c.inner.Checkpoint(ctx, workspace, options...)
}

func (c *Client) WorkspaceInfo(ctx context.Context, workspace string) (*api.WorkspaceInfo, error) {
	return c.inner.WorkspaceInfo(ctx, workspace)
}

func (c *Client) ListSessions(ctx context.Context, workspace string) ([]api.SessionStatus, error) {
	return c.inner.ListSessions(ctx, workspace)
}

// Chunk is one ordered piece of session output.
type Chunk = internalclient.Chunk

// Session is a durable cursor over a remote process or port connection.
// Reading Chunks advances the replay cursor; Close detaches unless kill=true.
type Session struct {
	inner *internalclient.Session
	ID    string
	WS    string
	Kind  string
}

func wrapSession(session *internalclient.Session) *Session {
	if session == nil {
		return nil
	}
	return &Session{inner: session, ID: session.ID, WS: session.WS, Kind: session.Kind}
}

func (c *Client) Exec(ctx context.Context, request api.SessionOpenRequest) (*Session, error) {
	session, err := c.inner.Exec(ctx, proto.SOpenReq{
		WS: request.WS, Kind: request.Kind, Program: append([]string(nil), request.Program...),
		Cwd: request.Cwd, Env: cloneStringMap(request.Env), Rows: request.Rows, Cols: request.Cols,
		Stdin: request.Stdin, TimeoutSec: request.TimeoutSec,
		IdempotencyKey: request.IdempotencyKey, NoSubscribe: request.NoSubscribe,
	})
	return wrapSession(session), err
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func (c *Client) OpenPort(ctx context.Context, workspace string, port int, options ...OperationOption) (*Session, error) {
	session, err := c.inner.OpenPort(ctx, workspace, port, options...)
	return wrapSession(session), err
}

func (c *Client) Attach(ctx context.Context, workspace, sessionID string, from uint64) (*Session, error) {
	session, err := c.inner.Attach(ctx, workspace, sessionID, from)
	return wrapSession(session), err
}

func (s *Session) Chunks() <-chan Chunk  { return s.inner.Chunks() }
func (s *Session) Exit() *api.ExitInfo   { return s.inner.Exit() }
func (s *Session) Done() <-chan struct{} { return s.inner.Done() }
func (s *Session) Next() uint64          { return s.inner.Next() }
func (s *Session) Err() error            { return s.inner.Err() }
func (s *Session) Input(ctx context.Context, data []byte, eof bool) error {
	return s.inner.Input(ctx, data, eof)
}
func (s *Session) Resize(ctx context.Context, rows, cols uint16) error {
	return s.inner.Resize(ctx, rows, cols)
}
func (s *Session) Signal(ctx context.Context, signal string) error {
	return s.inner.Signal(ctx, signal)
}
func (s *Session) Close(ctx context.Context, kill bool) error { return s.inner.Close(ctx, kill) }
func (s *Session) Wait(ctx context.Context) (*api.ExitInfo, error) {
	return s.inner.Wait(ctx)
}

func (c *Client) Run(ctx context.Context, workspace string, program ...string) (stdout, stderr []byte, exit *api.ExitInfo, err error) {
	return c.inner.Run(ctx, workspace, program...)
}

// Copy copies session output until exit or cancellation. Cancellation does not
// kill the remote process; the caller retains explicit ownership of Session.
func Copy(ctx context.Context, session *Session, stdout, stderr io.Writer) (*api.ExitInfo, error) {
	for {
		select {
		case chunk, ok := <-session.Chunks():
			if !ok {
				if err := session.Err(); err != nil {
					return session.Exit(), err
				}
				return session.Exit(), nil
			}
			var writer io.Writer
			switch chunk.Stream {
			case api.StreamStdout:
				writer = stdout
			case api.StreamStderr:
				writer = stderr
			case api.StreamGap:
				gap, gapErr := parseOutputGap(chunk.Data)
				if gapErr != nil {
					return session.Exit(), gapErr
				}
				if stderr != nil {
					if _, err := fmt.Fprintf(stderr, "\n[remount: output seq %d-%d elided]\n", gap.From, gap.To); err != nil {
						return session.Exit(), err
					}
				}
				return session.Exit(), outputGapEvictedError(gap)
			}
			if writer != nil {
				if _, err := writer.Write(chunk.Data); err != nil {
					return session.Exit(), err
				}
			}
		case <-ctx.Done():
			return session.Exit(), ctx.Err()
		}
	}
}

func parseOutputGap(data []byte) (api.Gap, error) {
	var gap api.Gap
	if err := proto.Unmarshal(data, &gap); err != nil || gap.To < gap.From {
		return api.Gap{}, fmt.Errorf("client: invalid session output gap")
	}
	return gap, nil
}

func outputGapEvictedError(gap api.Gap) error {
	return &api.Error{
		Code: api.CodeEvicted,
		Msg:  fmt.Sprintf("session output sequence %d-%d is no longer available", gap.From, gap.To),
	}
}

func (c *Client) Diag(ctx context.Context, verify bool) (*api.ControlDiagnostics, error) {
	return c.inner.Diag(ctx, verify)
}

func (c *Client) NodeDiag(ctx context.Context, nodeID, workspace string, verify bool) (*api.NodeDiagnostics, error) {
	return c.inner.NodeDiag(ctx, nodeID, workspace, verify)
}

func (c *Client) NodeStatus(ctx context.Context, nodeID string) (*api.NodeStatus, error) {
	return c.inner.NodeStatus(ctx, nodeID)
}
