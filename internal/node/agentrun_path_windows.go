//go:build windows

package node

import (
	"path/filepath"
	"strings"

	"remount.dev/remount/internal/proto"
)

func workspaceRelativePath(mount, name string) (string, error) {
	if name == "" {
		return "", proto.Err(proto.CodeBadRequest, "path is required")
	}
	if !filepath.IsAbs(name) {
		return filepath.ToSlash(filepath.Clean(name)), nil
	}
	relative, err := filepath.Rel(mount, name)
	if err != nil || relative == ".." || strings.HasPrefix(relative, `..\`) {
		return "", proto.Err(proto.CodeDenied, "path %q is outside the workspace", name)
	}
	return filepath.ToSlash(relative), nil
}
