package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"remount.dev/remount/internal/workspace/firecracker"
)

func cmdGuestAgent(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("guest-agent", flag.ContinueOnError)
	root := fs.String("workspace", "/workspace", "guest workspace mount")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !filepath.IsAbs(*root) {
		return errors.New("guest-agent workspace must be absolute")
	}
	return firecracker.ServeGuestAgent(ctx, *root)
}

func cmdGuestManifest(args []string) error {
	fs := flag.NewFlagSet("guest-manifest", flag.ContinueOnError)
	binary := fs.String("binary", "", "static remount binary installed in the guest")
	output := fs.String("output", "", "manifest path (default stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *binary == "" {
		return errors.New("guest-manifest requires --binary")
	}
	file, err := os.Open(*binary)
	if err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	manifest := firecracker.GuestManifest{
		Protocol: firecracker.GuestProtocolVersion, VsockPort: firecracker.GuestVsockPort,
		WorkspaceDir: "/workspace", BinarySHA256: hex.EncodeToString(h.Sum(nil)),
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if *output == "" {
		_, err = os.Stdout.Write(data)
		return err
	}
	if !filepath.IsAbs(*output) {
		return fmt.Errorf("guest manifest output %q must be absolute", *output)
	}
	fileOut, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := fileOut.Write(data); err != nil {
		fileOut.Close()
		return err
	}
	if err := fileOut.Sync(); err != nil {
		fileOut.Close()
		return err
	}
	return fileOut.Close()
}
