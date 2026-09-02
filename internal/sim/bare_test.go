package sim

import (
	"testing"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/eventlog"
)

// newBareControl builds a control plane with no artifact store, to check that
// a check which cannot run says so instead of reporting success.
func newBareControl(t *testing.T) (*eventlog.SQLite, *control.Control) {
	t.Helper()
	sq, err := eventlog.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sq.Close() })
	ctrl, err := control.New(control.Options{DB: sq.DB(), Log: eventlog.New(sq)})
	if err != nil {
		t.Fatal(err)
	}
	return sq, ctrl
}
