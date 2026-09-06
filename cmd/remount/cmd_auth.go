package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"strings"

	"golang.org/x/term"

	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/proto"
)

const authUsage = "auth login|status|logout RECIPE --ws WS"

func cmdAuth(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New(authUsage)
	}
	action := args[0]
	switch action {
	case proto.AuthActionLogin, proto.AuthActionStatus, proto.AuthActionLogout:
	default:
		return errors.New(authUsage)
	}
	fs := flag.NewFlagSet("auth "+action, flag.ExitOnError)
	var c common
	c.flags(fs)
	wsID := fs.String("ws", "", "existing trusted-local workspace")
	recipeFile := fs.String("recipe-file", "", "load recipe YAML from this file")
	parse(fs, args[1:])
	if fs.NArg() != 1 || *wsID == "" {
		return errors.New(authUsage)
	}
	recipe, _, err := loadRecipe(fs.Arg(0), *recipeFile)
	if err != nil {
		return err
	}
	if recipe.Subscription == nil {
		return errors.New("recipe " + recipe.Name + " does not support subscription authentication")
	}
	if _, err := c.ensureLocalServer(ctx); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	var rows, cols uint16 = 24, 80
	raw := action == proto.AuthActionLogin
	if raw && term.IsTerminal(int(os.Stdout.Fd())) {
		if w, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
			rows, cols = uint16(h), uint16(w)
		}
	}
	s, err := launch.StartAuth(ctx, cl, launch.AuthOptions{
		Recipe: recipe, WS: *wsID, Action: action, Rows: rows, Cols: cols, Stderr: os.Stderr,
	})
	if err != nil {
		return err
	}
	if !c.json && action == proto.AuthActionLogin {
		_, _ = os.Stderr.WriteString("subscription login is confidential and cannot be replayed or reattached\n")
	}
	err = drive(ctx, s, driveOptions{
		WS: *wsID, Session: s.ID, Raw: raw, ForwardStdin: raw, KillOnInterrupt: true,
	})
	if err != nil && strings.Contains(err.Error(), "reattach with") {
		return errors.New("provider auth interrupted; sensitive sessions cannot be reattached")
	}
	return err
}
