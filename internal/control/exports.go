package control

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

type exportCursorStore struct {
	control *Control
	tenant  string
}

// ExportCursors returns the durable cursor namespace for one tenant. Callers
// must derive tenant from an authenticated subject, never request parameters.
func (c *Control) ExportCursors(tenant string) (eventlog.CursorStore, error) {
	if strings.TrimSpace(tenant) == "" || len(tenant) > 256 {
		return nil, errors.New("control: export cursor tenant is required")
	}
	return &exportCursorStore{control: c, tenant: tenant}, nil
}

func validCursorName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for _, char := range name {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func (s *exportCursorStore) Load(ctx context.Context, name string) (eventlog.Cursor, error) {
	if err := ctx.Err(); err != nil {
		return eventlog.Cursor{}, err
	}
	if !validCursorName(name) {
		return eventlog.Cursor{}, errors.New("control: invalid export cursor name")
	}
	s.control.mu.Lock()
	defer s.control.mu.Unlock()
	var cursor eventlog.Cursor
	cursor.Name = name
	err := s.control.db.QueryRowContext(ctx, `SELECT next,revision,updated_at FROM export_cursors WHERE tenant=? AND name=?`, s.tenant, name).
		Scan(&cursor.Next, &cursor.Revision, &cursor.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return eventlog.Cursor{Name: name}, nil
	}
	return cursor, err
}

func (s *exportCursorStore) CompareAndSwap(ctx context.Context, name string, previous eventlog.Cursor, next uint64) (eventlog.Cursor, error) {
	if err := ctx.Err(); err != nil {
		return eventlog.Cursor{}, err
	}
	if !validCursorName(name) || previous.Name != name {
		return eventlog.Cursor{}, errors.New("control: invalid export cursor name")
	}
	if next < previous.Next {
		return previous, errors.New("control: export cursor cannot move backward")
	}
	if next > math.MaxInt64 {
		return previous, errors.New("control: export cursor sequence exhausted")
	}
	if previous.Revision >= math.MaxInt64 {
		return previous, errors.New("control: export cursor revision exhausted")
	}
	c := s.control
	c.mu.Lock()
	defer c.mu.Unlock()
	current := eventlog.Cursor{Name: name}
	advanced := eventlog.Cursor{}
	event := c.newEvent(proto.EvExportAdvanced, "export:"+name, "", "", map[string]any{
		"name": name, "from": previous.Next, "to": next, "revision": previous.Revision + 1,
	})
	event.Tenant = s.tenant
	err := c.transact(func(tx *eventlog.Tx) error {
		err := tx.QueryRow(`SELECT next,revision,updated_at FROM export_cursors WHERE tenant=? AND name=?`, s.tenant, name).
			Scan(&current.Next, &current.Revision, &current.UpdatedAt)
		absent := errors.Is(err, sql.ErrNoRows)
		if err != nil && !absent {
			return err
		}
		if absent {
			current = eventlog.Cursor{Name: name}
		}
		if current.Next != previous.Next || current.Revision != previous.Revision {
			return eventlog.ErrCursorConflict
		}
		if next < current.Next || current.Revision >= math.MaxInt64 {
			return errors.New("control: invalid export cursor advance")
		}
		if absent {
			var count int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM export_cursors WHERE tenant=?`, s.tenant).Scan(&count); err != nil {
				return err
			}
			if count >= c.opts.MaxExportCursorsPerTenant {
				return proto.Err(proto.CodeResourceExhausted, "tenant export cursor limit %d reached", c.opts.MaxExportCursorsPerTenant)
			}
		}
		advanced = eventlog.Cursor{Name: name, Next: next, Revision: current.Revision + 1, UpdatedAt: c.now().UnixMilli()}
		_, err = tx.Exec(`INSERT INTO export_cursors(tenant,name,next,revision,updated_at) VALUES(?,?,?,?,?)
ON CONFLICT(tenant,name) DO UPDATE SET next=excluded.next,revision=excluded.revision,updated_at=excluded.updated_at`,
			s.tenant, name, advanced.Next, advanced.Revision, advanced.UpdatedAt)
		return err
	}, []*proto.Event{event})
	if errors.Is(err, eventlog.ErrCursorConflict) {
		return current, eventlog.ErrCursorConflict
	}
	if err != nil {
		return current, err
	}
	return advanced, nil
}

var _ eventlog.CursorStore = (*exportCursorStore)(nil)
