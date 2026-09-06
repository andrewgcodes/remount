package launch

import (
	"context"
	"fmt"
	"io"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/providerauth"
)

// AuthOptions describes one confidential provider-native auth operation.
type AuthOptions struct {
	Recipe *Recipe
	WS     string
	Action string
	Rows   uint16
	Cols   uint16
	Stderr io.Writer
}

// StartAuth installs the provider CLI when needed and opens a sensitive
// active-client-only session using the node-approved provider command.
func StartAuth(ctx context.Context, cl *client.Client, o AuthOptions) (*client.Session, error) {
	if o.Recipe == nil || o.Recipe.Subscription == nil {
		return nil, fmt.Errorf("recipe does not support subscription authentication")
	}
	if o.WS == "" {
		return nil, fmt.Errorf("workspace id is required")
	}
	if err := (&proto.AuthOperation{Recipe: o.Recipe.Name, Action: o.Action}).Validate(); err != nil {
		return nil, err
	}
	ws, err := cl.GetWorkspace(ctx, o.WS)
	if err != nil {
		return nil, err
	}
	security, err := proto.NormalizeSecurity(ws.Spec.Security)
	if err != nil {
		return nil, err
	}
	if security.Profile != proto.SecurityLocal {
		return nil, fmt.Errorf("workspace %s has security profile %s, which forbids provider subscription login", ws.ID, security.Profile)
	}
	program, err := providerauth.Program(o.Recipe.Name, o.Action)
	if err != nil {
		return nil, err
	}
	out := o.Stderr
	if out == nil {
		out = io.Discard
	}
	data := Data{Recipe: o.Recipe.Name, Workspace: ws.ID, Auth: AuthSubscription}
	install, err := o.Recipe.InstallScript(data)
	if err != nil {
		return nil, err
	}
	if install != "" {
		if err := runInstall(ctx, cl, ws.ID, nil, install, out); err != nil {
			return nil, err
		}
	}
	kind := proto.SessionExec
	stdin := false
	timeout := 2 * time.Minute
	if o.Action == proto.AuthActionLogin {
		kind = proto.SessionPTY
		stdin = true
		timeout = 15 * time.Minute
	}
	return cl.Exec(ctx, proto.SOpenReq{
		WS: ws.ID, Kind: kind, Program: program,
		Rows: o.Rows, Cols: o.Cols, Stdin: stdin, TimeoutSec: int64(timeout.Seconds()),
		Sensitive: true, AuthOperation: &proto.AuthOperation{Recipe: o.Recipe.Name, Action: o.Action},
	})
}
