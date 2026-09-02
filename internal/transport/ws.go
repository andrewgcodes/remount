package transport

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"remount.dev/remount/internal/proto"
)

// wsConn carries one frame per binary WebSocket message.
type wsConn struct {
	c       *websocket.Conn
	wmu     sync.Mutex
	closeMu sync.Once
}

// NewWS wraps an accepted or dialed WebSocket.
func NewWS(c *websocket.Conn) Conn {
	c.SetReadLimit(MaxFrameBytes)
	return &wsConn{c: c}
}

// DialWS connects to a Remount relay URL (wss://host/v1/link).
func DialWS(ctx context.Context, url string, header http.Header) (Conn, error) {
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header, CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		return nil, err
	}
	return NewWS(c), nil
}

// AcceptWS upgrades an HTTP request.
func AcceptWS(w http.ResponseWriter, r *http.Request) (Conn, error) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode:    websocket.CompressionDisabled,
		InsecureSkipVerify: true, // origin checks are meaningless for non-browser peers; auth is the token
	})
	if err != nil {
		return nil, err
	}
	return NewWS(c), nil
}

func (w *wsConn) Send(ctx context.Context, f *proto.Frame) error {
	b, err := proto.EncodeFrame(f)
	if err != nil {
		return err
	}
	if len(b) > MaxFrameBytes {
		return errors.New("transport: frame too large")
	}
	w.wmu.Lock()
	defer w.wmu.Unlock()
	if err := w.c.Write(ctx, websocket.MessageBinary, b); err != nil {
		return mapWSErr(err)
	}
	return nil
}

func (w *wsConn) Recv(ctx context.Context) (*proto.Frame, error) {
	typ, b, err := w.c.Read(ctx)
	if err != nil {
		return nil, mapWSErr(err)
	}
	if typ != websocket.MessageBinary {
		return nil, errors.New("transport: non-binary message")
	}
	return proto.DecodeFrame(b)
}

func (w *wsConn) Close() error {
	w.closeMu.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = w.c.Close(websocket.StatusNormalClosure, "bye")
		_ = ctx
	})
	return nil
}

func mapWSErr(err error) error {
	if err == nil {
		return nil
	}
	var ce websocket.CloseError
	if errors.As(err, &ce) || websocket.CloseStatus(err) != -1 {
		return ErrClosed
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	// Any other read/write failure means the connection is unusable.
	return errors.Join(ErrClosed, err)
}
