package firecracker

import (
	"context"
	"errors"
	"fmt"
	"io"

	"remount.dev/remount/internal/fsops"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
)

// ServeGuestAgent serves the versioned host RPC protocol on AF_VSOCK. Only a
// kernel-authenticated host CID is accepted by the platform listener.
func ServeGuestAgent(ctx context.Context, workspaceRoot string) error {
	fs, err := fsops.New(workspaceRoot)
	if err != nil {
		return err
	}
	defer fs.Close()
	return serveGuestVsock(ctx, func(conn io.ReadWriteCloser) {
		defer conn.Close()
		_ = serveGuestConn(ctx, conn, fs)
	})
}

func serveGuestConn(ctx context.Context, conn io.ReadWriteCloser, fs *fsops.FS) error {
	frame, err := readGuest(conn)
	if err != nil {
		return err
	}
	if frame.Type != "req" {
		return writeGuest(conn, guestFrame{Type: "res", Err: proto.Err(proto.CodeBadRequest, "first guest frame must be a request")})
	}
	if frame.Op == "session.open" {
		return serveGuestSession(ctx, conn, frame.Body)
	}
	body, callErr := dispatchGuestFS(fs, frame.Op, frame.Body)
	return writeGuest(conn, guestFrame{Type: "res", Body: body, Err: guestWireError(callErr)})
}

func dispatchGuestFS(fs *fsops.FS, op string, body []byte) ([]byte, error) {
	marshal := func(value any, err error) ([]byte, error) {
		if err != nil {
			return nil, err
		}
		return proto.Marshal(value)
	}
	switch op {
	case "fs.read":
		var req proto.FSReadReq
		if err := proto.Unmarshal(body, &req); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "%v", err)
		}
		return marshal(fs.Read(req.Path, req.Offset, req.Limit))
	case "fs.write":
		var req proto.FSWriteReq
		if err := proto.Unmarshal(body, &req); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "%v", err)
		}
		return nil, fs.Write(req.Path, req.Data, req.Mode, req.Append, req.MkdirP)
	case "fs.list":
		var req proto.FSListReq
		if err := proto.Unmarshal(body, &req); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "%v", err)
		}
		entries, err := fs.List(req.Path)
		return marshal(proto.FSListRes{Entries: entries}, err)
	case "fs.stat":
		var req proto.FSStatReq
		if err := proto.Unmarshal(body, &req); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "%v", err)
		}
		entry, err := fs.Stat(req.Path)
		if err != nil {
			return nil, err
		}
		return proto.Marshal(proto.FSStatRes{Entry: *entry})
	case "fs.mkdir":
		var req proto.FSMkdirReq
		if err := proto.Unmarshal(body, &req); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "%v", err)
		}
		return nil, fs.Mkdir(req.Path)
	case "fs.remove":
		var req proto.FSRemoveReq
		if err := proto.Unmarshal(body, &req); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "%v", err)
		}
		return nil, fs.Remove(req.Path, req.Recursive)
	case "fs.rename":
		var req proto.FSRenameReq
		if err := proto.Unmarshal(body, &req); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "%v", err)
		}
		return nil, fs.Rename(req.From, req.To)
	case "fs.search":
		var req proto.FSSearchReq
		if err := proto.Unmarshal(body, &req); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "%v", err)
		}
		return marshal(fs.Search(req.Path, req.Pattern, req.Glob, req.MaxResults))
	case "fs.edit":
		var req proto.FSEditReq
		if err := proto.Unmarshal(body, &req); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "%v", err)
		}
		n, err := fs.Edit(req.Path, req.Edits)
		return marshal(proto.FSEditRes{Replacements: n}, err)
	default:
		return nil, proto.Err(proto.CodeUnsupported, "unknown guest operation %q", op)
	}
}

func guestWireError(err error) *proto.Error {
	if err == nil {
		return nil
	}
	var protocol *proto.Error
	if errors.As(err, &protocol) {
		return protocol
	}
	return proto.Err(proto.CodeInternal, "%v", err)
}

func serveGuestSession(ctx context.Context, conn io.ReadWriteCloser, body []byte) error {
	var spec session.Spec
	if err := proto.Unmarshal(body, &spec); err != nil {
		return writeGuest(conn, guestFrame{Type: "res", Err: proto.Err(proto.CodeBadRequest, "%v", err)})
	}
	spec.Runner = nil
	manager := session.NewManager(session.ManagerOptions{MaxSessions: 1, MaxActive: 1, MaxSessionsPerWorkspace: 1, MaxSessionsPerPrincipal: 1})
	defer manager.Close()
	s, err := manager.Open(spec)
	if err != nil {
		return writeGuest(conn, guestFrame{Type: "res", Err: guestWireError(err)})
	}
	opened, err := proto.Marshal(struct {
		PID int `cbor:"pid"`
	}{s.Info.PID})
	if err != nil {
		return err
	}
	if err := writeGuest(conn, guestFrame{Type: "res", Body: opened}); err != nil {
		return err
	}

	commandDone := make(chan error, 1)
	go func() { commandDone <- serveGuestCommands(conn, s) }()
	cursor := s.Log.CursorAt(0)
	for {
		chunks, err := cursor.Next(ctx, 32)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		for _, chunk := range chunks {
			switch chunk.Stream {
			case proto.StreamInfo:
				continue
			case proto.StreamExit:
				if err := writeGuest(conn, guestFrame{Type: "exit", Body: chunk.Data}); err != nil {
					return err
				}
				return nil
			default:
				if err := writeGuest(conn, guestFrame{Type: "chunk", Stream: chunk.Stream, Body: chunk.Data}); err != nil {
					return err
				}
			}
		}
		select {
		case commandErr := <-commandDone:
			if commandErr != nil {
				s.Terminate("guest transport closed")
				return commandErr
			}
		default:
		}
	}
}

func serveGuestCommands(conn io.Reader, s *session.Session) error {
	for {
		frame, err := readGuest(conn)
		if err != nil {
			return err
		}
		if frame.Type != "cmd" {
			return fmt.Errorf("unexpected guest session frame %q", frame.Type)
		}
		switch frame.Op {
		case "input":
			var req struct {
				Data []byte
				EOF  bool
			}
			if err := proto.Unmarshal(frame.Body, &req); err != nil {
				return err
			}
			if err := s.Input(0, req.Data, req.EOF); err != nil {
				return err
			}
		case "resize":
			var req struct{ Rows, Cols uint16 }
			if err := proto.Unmarshal(frame.Body, &req); err != nil {
				return err
			}
			if err := s.Resize(req.Rows, req.Cols); err != nil {
				return err
			}
		case "signal":
			var name string
			if err := proto.Unmarshal(frame.Body, &name); err != nil {
				return err
			}
			if err := s.Signal(name); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown guest session command %q", frame.Op)
		}
	}
}
