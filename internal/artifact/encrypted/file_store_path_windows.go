//go:build windows

package encrypted

import "strings"

func physicalObjectKey(key string) string {
	parts := strings.Split(key, "/")
	parts[len(parts)-1] = strings.TrimPrefix(parts[len(parts)-1], "art_sha256:")
	return strings.Join(parts, "/")
}

func logicalObjectKey(key string) string {
	parts := strings.Split(key, "/")
	if len(parts) == 4 && len(parts[3]) == 64 && !strings.Contains(parts[3], ":") {
		parts[3] = "art_sha256:" + parts[3]
	}
	return strings.Join(parts, "/")
}
