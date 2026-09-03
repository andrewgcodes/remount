package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/localfs"
)

// uploadDir packs dir and streams it into the control plane's artifact store
// in one pass. Warnings (a skipped oversized .git pack) go to stderr because
// they change what the agent will see.
func uploadDir(ctx context.Context, cl *client.Client, dir string, opts localfs.PackOptions) (string, localfs.Manifest, error) {
	pr, pw := io.Pipe()
	type packed struct {
		m   localfs.Manifest
		err error
	}
	done := make(chan packed, 1)
	go func() {
		m, err := localfs.Pack(dir, opts, pw)
		pw.CloseWithError(err)
		done <- packed{m, err}
	}()
	id, _, upErr := cl.UploadArtifact(ctx, pr)
	pr.Close()
	result := <-done
	if result.err != nil {
		return "", result.m, fmt.Errorf("pack %s: %w", dir, result.err)
	}
	if upErr != nil {
		return "", result.m, fmt.Errorf("upload: %w", upErr)
	}
	for _, w := range result.m.Warnings {
		fmt.Fprintln(os.Stderr, "warning:", w)
	}
	return id, result.m, nil
}

func cmdPush(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("push", flag.ExitOnError)
	var c common
	c.flags(fs)
	dir := fs.String("dir", ".", "local directory to upload")
	includeGit := fs.Bool("include-git", true, "include .git")
	var exclude listFlag
	fs.Var(&exclude, "exclude", "additional exclude glob (repeatable)")
	noIgnore := fs.Bool("no-ignore", false, "ignore .gitignore/.remountignore and the default excludes")
	parse(fs, args)
	if err := arity(fs, 1, 1, "push WS [--dir .] [--include-git=false] [--exclude GLOB]..."); err != nil {
		return err
	}
	wsID := fs.Arg(0)
	cl := c.client()
	defer cl.Close()
	id, m, err := uploadDir(ctx, cl, *dir, localfs.PackOptions{
		ExcludeGit: !*includeGit, Excludes: exclude, NoIgnoreFiles: *noIgnore, NoDefaultExcludes: *noIgnore,
	})
	if err != nil {
		return err
	}
	res, err := cl.ApplyTar(ctx, wsID, id)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(map[string]any{"ws": wsID, "artifact": id, "packed": m, "applied": res})
		return nil
	}
	fmt.Fprintf(os.Stderr, "pushed %d files (%d bytes) to %s\n", res.Files, res.Bytes, wsID)
	return nil
}

func cmdPull(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pull", flag.ExitOnError)
	var c common
	c.flags(fs)
	dir := fs.String("dir", ".", "local directory to update")
	force := fs.Bool("force", false, "overwrite even if the local tree has uncommitted changes")
	parse(fs, args)
	if err := arity(fs, 1, 1, "pull WS [--dir .] [--force]"); err != nil {
		return err
	}
	wsID := fs.Arg(0)
	cl := c.client()
	defer cl.Close()
	snap, err := cl.Snapshot(ctx, wsID, true)
	if err != nil {
		return err
	}
	rc, err := cl.DownloadArtifact(ctx, snap.Artifact)
	if err != nil {
		return err
	}
	defer rc.Close()
	res, err := localfs.Unpack(*dir, rc, localfs.UnpackOptions{Force: *force})
	if errors.Is(err, localfs.ErrDirty) {
		return fmt.Errorf("%s has uncommitted changes; commit or stash them, or pass --force", *dir)
	}
	if err != nil {
		return err
	}
	if c.json {
		printJSON(map[string]any{"ws": wsID, "artifact": snap.Artifact, "result": res})
		return nil
	}
	fmt.Fprintf(os.Stderr, "pulled %s: %d written, %d unchanged", wsID, len(res.Written), res.Unchanged)
	if len(res.Extra) > 0 {
		fmt.Fprintf(os.Stderr, ", %d local-only left in place", len(res.Extra))
	}
	if len(res.Skipped) > 0 {
		fmt.Fprintf(os.Stderr, ", skipped %s", strings.Join(res.Skipped, " "))
	}
	fmt.Fprintln(os.Stderr)
	return nil
}
