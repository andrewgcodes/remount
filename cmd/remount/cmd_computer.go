package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

const computerUsage = "computer create WS [--viewport WxH] [--profile NAME] [--port N] [--attach] [-- program args…] | " +
	"computer get|screenshot|downloads|close WS COMPUTER | computer click|scroll WS COMPUTER X Y … | " +
	"computer type WS COMPUTER TEXT | computer key WS COMPUTER KEY | computer navigate WS COMPUTER URL | " +
	"computer eval WS COMPUTER EXPRESSION"

// cmdComputer drives one browser session over the port substrate. Coordinates
// are CSS pixels from the top-left of the viewport declared at create, which
// is the same rectangle a screenshot is clipped to (ADR 0088).
func cmdComputer(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New(computerUsage)
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("computer "+sub, flag.ExitOnError)
	var commonFlags common
	commonFlags.flags(fs)
	switch sub {
	case "create":
		return computerCreate(ctx, fs, &commonFlags, rest)
	case "get":
		return computerGet(ctx, fs, &commonFlags, rest)
	case "screenshot":
		return computerScreenshot(ctx, fs, &commonFlags, rest)
	case "click", "type", "key", "scroll":
		return computerInput(ctx, fs, &commonFlags, sub, rest)
	case "navigate":
		return computerNavigate(ctx, fs, &commonFlags, rest)
	case "eval":
		return computerEval(ctx, fs, &commonFlags, rest)
	case "downloads":
		return computerDownloads(ctx, fs, &commonFlags, rest)
	case "close":
		return computerClose(ctx, fs, &commonFlags, rest)
	default:
		return fmt.Errorf("unknown computer subcommand %q", sub)
	}
}

func computerCreate(ctx context.Context, fs *flag.FlagSet, c *common, args []string) error {
	viewport := fs.String("viewport", "", "viewport as WxH in CSS pixels (default 1280x720)")
	profile := fs.String("profile", "", "browser profile name (default \"default\")")
	port := fs.Int("port", 0, "DevTools port inside the workspace (default 9222)")
	attach := fs.Bool("attach", false, "attach to a browser the workspace already started")
	env := kvFlag{}
	fs.Var(env, "env", "extra env K=V for the browser process (repeatable)")
	idem := fs.String("idem", "", "stable idempotency key")
	parse(fs, args)
	if err := arity(fs, 1, -1, computerUsage); err != nil {
		return err
	}
	req := proto.ComputerCreateReq{WS: fs.Arg(0), Profile: *profile, Env: env}
	if *viewport != "" {
		w, h, err := parseViewport(*viewport)
		if err != nil {
			return err
		}
		req.Viewport = proto.ComputerViewport{Width: w, Height: h}
	}
	program := fs.Args()[1:]
	if len(program) > 0 && program[0] == "--" {
		program = program[1:]
	}
	if *port != 0 || *attach || len(program) > 0 {
		req.Launch = &proto.ComputerLaunch{Program: program, Port: *port, Attach: *attach}
	}
	cl := c.client()
	defer cl.Close()
	var options []client.OperationOption
	if *idem != "" {
		options = append(options, client.WithIdempotencyKey(*idem))
	}
	computer, err := cl.CreateComputer(ctx, req, options...)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(map[string]any{
			"computer": computer.ID(), "ws": computer.WS(), "s": computer.Session(),
			"cdp": computer.CDPVersion(),
			"w":   computer.Viewport().Width, "h": computer.Viewport().Height,
		})
		return nil
	}
	fmt.Printf("%s\t%s\t%dx%d\t%s\n", computer.ID(), computer.Session(),
		computer.Viewport().Width, computer.Viewport().Height, computer.CDPVersion())
	return nil
}

func computerGet(ctx context.Context, fs *flag.FlagSet, c *common, args []string) error {
	parse(fs, args)
	if err := arity(fs, 2, 2, computerUsage); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	res, err := cl.Computer(fs.Arg(0), fs.Arg(1)).Get(ctx)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(res)
		return nil
	}
	// The reason is what separates a browser that crashed from one a caller
	// closed, so it is printed even when it is empty.
	fmt.Printf("%s\t%s\t%s\t%dx%d\t%s\n", res.Computer, res.State, res.Reason,
		res.Viewport.Width, res.Viewport.Height, res.Session)
	return nil
}

func computerScreenshot(ctx context.Context, fs *flag.FlagSet, c *common, args []string) error {
	out := fs.String("out", "", "write the PNG to this file (required)")
	parse(fs, args)
	if err := arity(fs, 2, 2, computerUsage); err != nil {
		return err
	}
	if *out == "" {
		// A PNG on a terminal is line noise, and a caller who wanted bytes on
		// stdout would rather be told than have their scrollback corrupted.
		return errors.New("computer screenshot needs --out FILE.png")
	}
	cl := c.client()
	defer cl.Close()
	res, err := cl.Computer(fs.Arg(0), fs.Arg(1)).Screenshot(ctx)
	if err != nil {
		return err
	}
	if err := os.WriteFile(*out, res.PNG, 0o600); err != nil {
		return err
	}
	if c.json {
		printJSON(map[string]any{"file": *out, "bytes": len(res.PNG), "w": res.Width, "h": res.Height})
		return nil
	}
	fmt.Printf("%s\t%d bytes\t%dx%d\n", *out, len(res.PNG), res.Width, res.Height)
	return nil
}

// computerInput covers the four action subcommands. They share one request so
// the input sequence, and therefore the at-most-once guarantee, is identical.
func computerInput(ctx context.Context, fs *flag.FlagSet, c *common, kind string, args []string) error {
	var modifiers modifierFlags
	modifiers.flags(fs)
	parse(fs, args)
	var action proto.ComputerAction
	switch kind {
	case "click":
		if err := arity(fs, 4, 4, "computer click WS COMPUTER X Y"); err != nil {
			return err
		}
		x, y, err := parseCoords(fs.Arg(2), fs.Arg(3))
		if err != nil {
			return err
		}
		action = proto.ComputerAction{Kind: proto.ComputerActionClick, X: x, Y: y}
	case "scroll":
		if err := arity(fs, 6, 6, "computer scroll WS COMPUTER X Y DX DY"); err != nil {
			return err
		}
		x, y, err := parseCoords(fs.Arg(2), fs.Arg(3))
		if err != nil {
			return err
		}
		dx, dy, err := parseCoords(fs.Arg(4), fs.Arg(5))
		if err != nil {
			return err
		}
		action = proto.ComputerAction{Kind: proto.ComputerActionScroll, X: x, Y: y, DX: dx, DY: dy}
	case "type":
		if err := arity(fs, 3, 3, "computer type WS COMPUTER TEXT"); err != nil {
			return err
		}
		action = proto.ComputerAction{Kind: proto.ComputerActionType, Text: fs.Arg(2)}
	case "key":
		if err := arity(fs, 3, 3, "computer key WS COMPUTER KEY"); err != nil {
			return err
		}
		action = proto.ComputerAction{Kind: proto.ComputerActionKey, Key: fs.Arg(2)}
	}
	action.Modifiers = modifiers.bits()
	cl := c.client()
	defer cl.Close()
	if err := cl.Computer(fs.Arg(0), fs.Arg(1)).Input(ctx, action); err != nil {
		return err
	}
	if c.json {
		printJSON(map[string]any{"computer": fs.Arg(1), "applied": true, "kind": action.Kind})
	}
	return nil
}

func computerNavigate(ctx context.Context, fs *flag.FlagSet, c *common, args []string) error {
	idem := fs.String("idem", "", "stable idempotency key")
	parse(fs, args)
	if err := arity(fs, 3, 3, "computer navigate WS COMPUTER URL"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	var options []client.OperationOption
	if *idem != "" {
		options = append(options, client.WithIdempotencyKey(*idem))
	}
	res, err := cl.Computer(fs.Arg(0), fs.Arg(1)).Navigate(ctx, fs.Arg(2), options...)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(res)
		return nil
	}
	fmt.Printf("%s\t%s\t%s\n", res.Status, res.URL, res.Title)
	return nil
}

func computerEval(ctx context.Context, fs *flag.FlagSet, c *common, args []string) error {
	parse(fs, args)
	if err := arity(fs, 3, 3, "computer eval WS COMPUTER EXPRESSION"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	value, err := cl.Computer(fs.Arg(0), fs.Arg(1)).Eval(ctx, fs.Arg(2))
	if err != nil {
		return err
	}
	// The value is page-controlled JSON. It is printed, never executed, and
	// --json nests it so a script can tell the value from the envelope.
	if c.json {
		printJSON(map[string]any{"computer": fs.Arg(1), "value": json.RawMessage(value)})
		return nil
	}
	if len(value) == 0 {
		return nil
	}
	fmt.Println(string(value))
	return nil
}

func computerDownloads(ctx context.Context, fs *flag.FlagSet, c *common, args []string) error {
	parse(fs, args)
	if err := arity(fs, 2, 2, computerUsage); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	downloads, err := cl.Computer(fs.Arg(0), fs.Arg(1)).Downloads(ctx)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(downloads)
		return nil
	}
	tw := tabWriter()
	fmt.Fprintln(tw, "STATE\tFILENAME\tBYTES\tARTIFACT\tREASON")
	for _, d := range downloads {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\n", d.State, d.Filename, d.Bytes, d.Artifact, d.Reason)
	}
	tw.Flush()
	return nil
}

func computerClose(ctx context.Context, fs *flag.FlagSet, c *common, args []string) error {
	idem := fs.String("idem", "", "stable idempotency key")
	parse(fs, args)
	if err := arity(fs, 2, 2, computerUsage); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	var options []client.OperationOption
	if *idem != "" {
		options = append(options, client.WithIdempotencyKey(*idem))
	}
	if err := cl.Computer(fs.Arg(0), fs.Arg(1)).Close(ctx, options...); err != nil {
		return err
	}
	if c.json {
		printJSON(map[string]string{"closed": fs.Arg(1)})
	}
	return nil
}

// modifierFlags turns the four named modifier flags into CDP's bit field so a
// caller never has to know the numbers.
type modifierFlags struct{ alt, ctrl, meta, shift bool }

func (m *modifierFlags) flags(fs *flag.FlagSet) {
	fs.BoolVar(&m.alt, "alt", false, "hold Alt")
	fs.BoolVar(&m.ctrl, "ctrl", false, "hold Control")
	fs.BoolVar(&m.meta, "meta", false, "hold Meta (Command)")
	fs.BoolVar(&m.shift, "shift", false, "hold Shift")
}

func (m *modifierFlags) bits() int {
	bits := 0
	if m.alt {
		bits |= proto.ComputerModifierAlt
	}
	if m.ctrl {
		bits |= proto.ComputerModifierCtrl
	}
	if m.meta {
		bits |= proto.ComputerModifierMeta
	}
	if m.shift {
		bits |= proto.ComputerModifierShift
	}
	return bits
}

// parseViewport reads WxH. It rejects anything else rather than guessing: a
// viewport that is not what the caller meant moves every later coordinate.
func parseViewport(s string) (int, int, error) {
	width, height, ok := strings.Cut(strings.ToLower(s), "x")
	if !ok {
		return 0, 0, fmt.Errorf("--viewport must be WxH, got %q", s)
	}
	w, err := strconv.Atoi(strings.TrimSpace(width))
	if err != nil || w <= 0 {
		return 0, 0, fmt.Errorf("--viewport width must be a positive integer, got %q", width)
	}
	h, err := strconv.Atoi(strings.TrimSpace(height))
	if err != nil || h <= 0 {
		return 0, 0, fmt.Errorf("--viewport height must be a positive integer, got %q", height)
	}
	return w, h, nil
}

func parseCoords(first, second string) (int, int, error) {
	a, err := strconv.Atoi(first)
	if err != nil {
		return 0, 0, fmt.Errorf("expected an integer coordinate, got %q", first)
	}
	b, err := strconv.Atoi(second)
	if err != nil {
		return 0, 0, fmt.Errorf("expected an integer coordinate, got %q", second)
	}
	return a, b, nil
}
