package sdks

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/relay"
	"remount.dev/remount/internal/transport"
)

// This gate requires built language packages, but no cloud, model, or secrets.
// The ordinary Go suite reports it unavailable unless explicitly selected.
func TestSDKStrictProfileProtocol(t *testing.T) {
	if os.Getenv("REMOUNT_SDK_CHECK") != "1" {
		t.Skip("cross-language gate unavailable: build SDKs and set REMOUNT_SDK_CHECK=1")
	}
	python := os.Getenv("REMOUNT_SDK_PYTHON")
	if python == "" {
		python = "python3"
	}
	for _, profile := range []string{proto.SecurityIsolated, proto.SecurityMultiTenant} {
		t.Run(profile, func(t *testing.T) {
			sq, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "control.db"))
			if err != nil {
				t.Fatal(err)
			}
			log := eventlog.New(sq)
			defer log.Close()
			c, err := control.New(control.Options{DB: sq.DB(), Log: log, SecurityProfileFloor: profile,
				Authenticator: control.StaticAuthenticator{"sdk-synthetic-token": {ID: "sdk-test", Tenant: "sdk-test"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			r := relay.New(c)
			c.Attach(r)
			c.Start()
			defer c.Stop()
			defer r.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				conn, err := transport.AcceptWS(w, req)
				if err != nil {
					return
				}
				_ = r.Serve(ctx, conn)
			}))
			defer httpServer.Close()
			for _, language := range []struct{ name, executable, script string }{
				{"python", python, "protocol_smoke.py"}, {"typescript", "node", "protocol_smoke.mjs"},
			} {
				t.Run(language.name, func(t *testing.T) {
					cmd := exec.CommandContext(ctx, language.executable, language.script, httpServer.URL, profile)
					cmd.Env = append(os.Environ(), "PYTHONPATH=../../sdk/python/src")
					out, err := cmd.CombinedOutput()
					if err != nil {
						t.Fatalf("%s: %v\n%s", language.name, err, out)
					}
					t.Logf("%s", out)
				})
			}
		})
	}
}
