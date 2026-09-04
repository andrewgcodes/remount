//go:build windows

package fsops

import (
	"path/filepath"
	"strings"

	"remount.dev/remount/internal/proto"
)

func validatePlatformPath(name string) error {
	if filepath.VolumeName(name) != "" {
		return proto.Err(proto.CodeBadRequest, "path contains a Windows volume")
	}
	for _, part := range strings.Split(strings.ReplaceAll(name, "\\", "/"), "/") {
		if part == "" || part == "." || part == ".." {
			continue
		}
		if strings.Contains(part, ":") {
			return proto.Err(proto.CodeBadRequest, "path contains a Windows alternate data stream")
		}
		if strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") {
			return proto.Err(proto.CodeBadRequest, "path contains a Windows-ambiguous trailing character")
		}
		base, _, _ := strings.Cut(part, ".")
		switch strings.ToUpper(base) {
		case "CON", "PRN", "AUX", "NUL", "CLOCK$",
			"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
			"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
			return proto.Err(proto.CodeBadRequest, "path contains a reserved Windows device name")
		}
	}
	return nil
}
