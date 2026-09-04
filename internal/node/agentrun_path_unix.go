//go:build !windows

package node

import (
	"path"
	"strings"

	"remount.dev/remount/internal/proto"
)

func workspaceRelativePath(mount, name string) (string, error) {
	if name == "" {
		return "", proto.Err(proto.CodeBadRequest, "path is required")
	}
	if !strings.HasPrefix(name, "/") {
		return path.Clean(name), nil
	}
	mount = strings.TrimSuffix(mount, "/")
	if name == mount {
		return ".", nil
	}
	if !strings.HasPrefix(name, mount+"/") {
		return "", proto.Err(proto.CodeDenied, "path %q is outside the workspace", name)
	}
	return path.Clean(strings.TrimPrefix(name, mount+"/")), nil
}
