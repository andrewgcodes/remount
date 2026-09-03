package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"remount.dev/remount/internal/artifact/chunked"
	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/localfs"
	"remount.dev/remount/internal/proto"
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

// uploadDirChunked publishes dir as a chunked manifest, transferring only the
// content the control plane does not already hold. The file set comes from the
// same selector Pack uses, so --chunked can never upload something the tar
// path would have ignored.
func uploadDirChunked(ctx context.Context, cl *client.Client, dir string, opts localfs.PackOptions) (chunked.SnapshotResult, error) {
	if len(opts.Extra) > 0 {
		return chunked.SnapshotResult{}, errors.New("a chunked upload carries no trees from outside the directory")
	}
	sel, err := localfs.Select(dir, opts)
	if err != nil {
		return chunked.SnapshotResult{}, err
	}
	defer sel.Close()
	for _, w := range sel.Warnings {
		fmt.Fprintln(os.Stderr, "warning:", w)
	}
	result, err := cl.UploadChunkedSnapshot(ctx, sel.Dir, chunked.SnapshotOptions{Skip: sel.Skip})
	if err != nil {
		return result, fmt.Errorf("chunked upload %s: %w", dir, err)
	}
	return result, nil
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
	useChunks := fs.Bool("chunked", false, "upload deduplicated chunks; only content the server does not already hold is transferred")
	parse(fs, args)
	if err := arity(fs, 1, 1, "push WS [--dir .] [--include-git=false] [--exclude GLOB]... [--chunked]"); err != nil {
		return err
	}
	wsID := fs.Arg(0)
	packOpts := localfs.PackOptions{
		ExcludeGit: !*includeGit, Excludes: exclude, NoIgnoreFiles: *noIgnore, NoDefaultExcludes: *noIgnore,
	}
	cl := c.client()
	defer cl.Close()
	var (
		id     string
		format = proto.ArtifactFormatTar
		packed localfs.Manifest
		dedup  *chunked.SnapshotResult
	)
	if *useChunks {
		result, err := uploadDirChunked(ctx, cl, *dir, packOpts)
		if err != nil {
			return err
		}
		id, format, dedup = result.ManifestID, proto.ArtifactFormatChunkedV1, &result
	} else {
		uploaded, m, err := uploadDir(ctx, cl, *dir, packOpts)
		if err != nil {
			return err
		}
		id, packed = uploaded, m
	}
	res, err := cl.ApplyArtifact(ctx, wsID, id, format)
	if err != nil {
		return err
	}
	if c.json {
		out := map[string]any{"ws": wsID, "artifact": id, "format": format, "applied": res}
		// Deduplication is the only reason to choose --chunked, so what it
		// saved is part of the result rather than a log line.
		if dedup != nil {
			out["chunked"] = dedup
		} else {
			out["packed"] = packed
		}
		printJSON(out)
		return nil
	}
	fmt.Fprintf(os.Stderr, "pushed %d files (%d bytes) to %s\n", res.Files, res.Bytes, wsID)
	if dedup != nil {
		fmt.Fprintf(os.Stderr, "chunked: uploaded %d of %d chunks, %d bytes transferred of %d\n",
			dedup.ChunksUploaded, dedup.Chunks, dedup.BytesUploaded, dedup.PlaintextBytes)
	}
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
	rc, err := cl.DownloadSnapshot(ctx, snap.Artifact, snap.Format)
	if err != nil {
		return err
	}
	res, err := localfs.Unpack(*dir, rc, localfs.UnpackOptions{Force: *force})
	closeErr := rc.Close()
	err = errors.Join(err, closeErr)
	if errors.Is(err, localfs.ErrDirty) {
		return fmt.Errorf("%s has uncommitted changes; commit or stash them, or pass --force", *dir)
	}
	if err != nil {
		return err
	}
	if c.json {
		// The representation is reported because it decides how much moved:
		// a chunked snapshot is reconstructed from chunks rather than fetched
		// as one archive.
		printJSON(map[string]any{"ws": wsID, "artifact": snap.Artifact, "format": snap.Format, "result": res})
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

// ---------------------------------------------------------------------------
// bases: named, GC-pinned snapshots
// ---------------------------------------------------------------------------

func cmdBase(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("base: ls|rm NAME  (create one with: remount ws snapshot WS --as-base NAME)")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("base "+sub, flag.ExitOnError)
	var c common
	c.flags(fs)
	switch sub {
	case "ls":
		parse(fs, rest)
		if err := arity(fs, 0, 0, "base ls"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		bases, err := cl.ListBases(ctx)
		if err != nil {
			return err
		}
		if c.json {
			printJSON(bases)
			return nil
		}
		tw := tabWriter()
		fmt.Fprintln(tw, "NAME\tTENANT\tOWNER\tARTIFACT\tBYTES\tFROM\tCREATED")
		for _, b := range bases {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n", b.Name, b.Tenant, b.Owner, short(b.Artifact), b.Bytes, b.Workspace,
				time.UnixMilli(b.CreatedAt).UTC().Format(time.RFC3339))
		}
		tw.Flush()
	case "rm":
		idem := fs.String("idem", "", "stable idempotency key for retry after an ambiguous result")
		parse(fs, rest)
		if err := arity(fs, 1, 1, "base rm NAME"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		var opts []client.OperationOption
		if *idem != "" {
			opts = append(opts, client.WithIdempotencyKey(*idem))
		}
		if err := cl.RemoveBase(ctx, fs.Arg(0), opts...); err != nil {
			return err
		}
		if c.json {
			printJSON(map[string]string{"removed": fs.Arg(0)})
		}
	default:
		return fmt.Errorf("unknown base subcommand %q", sub)
	}
	return nil
}

// ---------------------------------------------------------------------------
// volumes: immutable shared data mounted read-only into workspaces
// ---------------------------------------------------------------------------

func cmdVolume(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("volume: create ID --dir PATH | ls | get ID | rm ID | attach ID WS PATH | detach WS PATH | publish WS PATH")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("volume "+sub, flag.ExitOnError)
	var c common
	c.flags(fs)
	idem := fs.String("idem", "", "stable idempotency key for retry after an ambiguous result")
	operationOptions := func() []client.OperationOption {
		if *idem == "" {
			return nil
		}
		return []client.OperationOption{client.WithIdempotencyKey(*idem)}
	}
	switch sub {
	case "create":
		dir := fs.String("dir", "", "local directory to publish as version 1")
		var exclude listFlag
		fs.Var(&exclude, "exclude", "exclude glob (repeatable)")
		parse(fs, rest)
		if err := arity(fs, 1, 1, "volume create ID --dir PATH"); err != nil {
			return err
		}
		if *dir == "" {
			return errors.New("volume create requires --dir")
		}
		cl := c.client()
		defer cl.Close()
		artifactID, manifest, err := uploadDir(ctx, cl, *dir, localfs.PackOptions{ExcludeGit: true, Excludes: exclude})
		if err != nil {
			return err
		}
		volume, err := cl.CreateVolume(ctx, proto.VolumeCreateReq{ID: fs.Arg(0), Artifact: artifactID}, operationOptions()...)
		if err != nil {
			return err
		}
		if c.json {
			printJSON(map[string]any{"volume": volume, "packed": manifest})
		} else {
			fmt.Printf("%s\tversion=%d\tartifact=%s\n", volume.ID, volume.Version, volume.Artifact)
		}
	case "ls":
		parse(fs, rest)
		if err := arity(fs, 0, 0, "volume ls"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		volumes, err := cl.ListVolumes(ctx)
		if err != nil {
			return err
		}
		if c.json {
			printJSON(volumes)
			return nil
		}
		tw := tabWriter()
		fmt.Fprintln(tw, "ID\tTENANT\tOWNER\tVERSION\tARTIFACT\tUPDATED")
		for _, volume := range volumes {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\n", volume.ID, volume.Tenant, volume.Owner, volume.Version, short(volume.Artifact), time.UnixMilli(volume.UpdatedAt).UTC().Format(time.RFC3339))
		}
		tw.Flush()
	case "get":
		parse(fs, rest)
		if err := arity(fs, 1, 1, "volume get ID"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		volume, err := cl.GetVolume(ctx, fs.Arg(0))
		if err != nil {
			return err
		}
		if c.json {
			printJSON(volume)
		} else {
			fmt.Printf("%s\tversion=%d\tartifact=%s\n", volume.ID, volume.Version, volume.Artifact)
		}
	case "rm":
		parse(fs, rest)
		if err := arity(fs, 1, 1, "volume rm ID"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		if err := cl.RemoveVolume(ctx, fs.Arg(0), operationOptions()...); err != nil {
			return err
		}
		if c.json {
			printJSON(map[string]string{"removed": fs.Arg(0)})
		}
	case "attach":
		parse(fs, rest)
		if err := arity(fs, 3, 3, "volume attach ID WS PATH"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		ws, err := cl.GetWorkspace(ctx, fs.Arg(1))
		if err != nil {
			return err
		}
		ws, err = cl.AttachVolume(ctx, proto.VolumeAttachReq{ID: fs.Arg(0), Workspace: ws.ID, Generation: ws.Generation, Path: fs.Arg(2)}, operationOptions()...)
		if err != nil {
			return err
		}
		if c.json {
			printJSON(ws)
		}
	case "detach":
		parse(fs, rest)
		if err := arity(fs, 2, 2, "volume detach WS PATH"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		ws, err := cl.GetWorkspace(ctx, fs.Arg(0))
		if err != nil {
			return err
		}
		ws, err = cl.DetachVolume(ctx, proto.VolumeDetachReq{Workspace: ws.ID, Generation: ws.Generation, Path: fs.Arg(1)}, operationOptions()...)
		if err != nil {
			return err
		}
		if c.json {
			printJSON(ws)
		}
	case "publish":
		volumeID := fs.String("volume", "", "volume ID (required when PATH is not its current mount path)")
		parse(fs, rest)
		if err := arity(fs, 2, 2, "volume publish WS PATH [--volume ID]"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		ws, err := cl.GetWorkspace(ctx, fs.Arg(0))
		if err != nil {
			return err
		}
		if *volumeID == "" {
			for i := range ws.Spec.Volumes {
				if ws.Spec.Volumes[i].Path == fs.Arg(1) {
					*volumeID = ws.Spec.Volumes[i].ID
					break
				}
			}
			if *volumeID == "" {
				return fmt.Errorf("PATH is not a mounted volume; select the target with --volume ID")
			}
		}
		current, err := cl.GetVolume(ctx, *volumeID)
		if err != nil {
			return err
		}
		volume, err := cl.PublishVolumePath(ctx, ws.ID, fs.Arg(1), current.ID, current.Version, operationOptions()...)
		if err != nil {
			return err
		}
		if c.json {
			printJSON(volume)
		} else {
			fmt.Printf("%s\tversion=%d\tartifact=%s\n", volume.ID, volume.Version, volume.Artifact)
		}
	default:
		return fmt.Errorf("unknown volume subcommand %q", sub)
	}
	return nil
}
