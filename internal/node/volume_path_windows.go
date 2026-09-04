//go:build windows

package node

import (
	"strings"

	"remount.dev/remount/internal/artifact"
)

func volumeArtifactName(id string) string {
	return strings.TrimPrefix(id, artifact.Prefix)
}

func artifactIDFromVolumeName(name string) string {
	return artifact.Prefix + name
}
