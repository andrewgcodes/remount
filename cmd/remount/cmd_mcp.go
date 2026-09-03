package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	remountmcp "remount.dev/remount/internal/mcp"
)

const mcpUsage = `mcp serve [--server URL --token TOKEN] [--http 127.0.0.1:PORT --mcp-token TOKEN]
    mcp config claude|codex|cursor|goose|gemini|opencode|agents [--command PATH]
    mcp wrap --broker URL --secret-env ENV=BINDING... -- SERVER [ARGS...]`

func cmdMCP(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New(mcpUsage)
	}
	switch args[0] {
	case "serve":
		return cmdMCPServe(ctx, args[1:])
	case "config":
		return cmdMCPConfig(args[1:])
	case "wrap":
		return cmdMCPWrap(ctx, args[1:])
	default:
		return fmt.Errorf("mcp: unknown subcommand %q\n%s", args[0], mcpUsage)
	}
}

func cmdMCPServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("mcp serve", flag.ContinueOnError)
	var c common
	var listen, mcpToken string
	var maxMessage, maxActive int
	var origins listFlag
	c.flags(fs)
	fs.StringVar(&listen, "http", "", "serve stateless MCP 2026-07-28 HTTP on this address instead of stdio")
	fs.StringVar(&mcpToken, "mcp-token", os.Getenv("REMOUNT_MCP_TOKEN"), "bearer token required by the MCP HTTP endpoint")
	fs.IntVar(&maxMessage, "max-message", 1<<20, "maximum JSON-RPC request/result bytes")
	fs.IntVar(&maxActive, "max-active", 16, "maximum concurrent tool calls")
	fs.Var(&origins, "allow-origin", "allowed HTTP Origin (repeatable; loopback is always allowed)")
	parse(fs, args)
	if err := arity(fs, 0, 0, "mcp serve [flags]"); err != nil {
		return err
	}
	if maxMessage < 4096 || maxMessage > 16<<20 {
		return errors.New("--max-message must be between 4096 and 16777216")
	}
	if maxActive < 1 || maxActive > 256 {
		return errors.New("--max-active must be between 1 and 256")
	}
	cli := c.client()
	defer cli.Close()
	server := remountmcp.NewServer(remountmcp.Options{Gateway: &remountmcp.ClientGateway{Client: cli}, MaxMessageBytes: maxMessage,
		MaxActive: maxActive, Version: version, BearerToken: mcpToken, AllowedOrigins: origins})
	if listen == "" {
		return server.ServeStdio(ctx, os.Stdin, os.Stdout)
	}
	if !loopbackListen(listen) && mcpToken == "" {
		return errors.New("--mcp-token is required when --http is not loopback")
	}
	httpServer := &http.Server{Addr: listen, Handler: server, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 2 * time.Minute}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = httpServer.Shutdown(shutdownCtx)
			cancel()
		case <-done:
		}
	}()
	err := httpServer.ListenAndServe()
	close(done)
	if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func loopbackListen(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func cmdMCPConfig(args []string) error {
	fs := flag.NewFlagSet("mcp config", flag.ContinueOnError)
	command := fs.String("command", "remount", "remount executable path")
	parse(fs, args)
	if err := arity(fs, 1, 1, "mcp config HOST [--command PATH]"); err != nil {
		return err
	}
	snippet, err := remountmcp.ConfigSnippet(fs.Arg(0), *command)
	if err != nil {
		return err
	}
	fmt.Print(snippet)
	return nil
}

func cmdMCPWrap(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("mcp wrap", flag.ContinueOnError)
	broker := fs.String("broker", os.Getenv("REMOUNT_BROKER"), "workspace broker URL (never read from .remount/env)")
	secrets := kvFlag{}
	fs.Var(secrets, "secret-env", "ENV=BINDING to replace with ref:BINDING (repeatable)")
	parse(fs, args)
	if err := arity(fs, 1, -1, "mcp wrap [flags] -- SERVER [ARGS...]"); err != nil {
		return err
	}
	return remountmcp.Wrap(ctx, remountmcp.WrapOptions{Command: fs.Args(), Broker: *broker, SecretEnv: secrets,
		Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr})
}
