//go:build !windows

package node

func volumeArtifactName(id string) string { return id }

func artifactIDFromVolumeName(name string) string { return name }
