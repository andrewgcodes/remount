//go:build windows

package storage_test

import "strings"

func physicalArtifactSuffix(id string) string {
	return strings.TrimPrefix(id, "art_sha256:")
}
