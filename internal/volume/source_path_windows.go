//go:build windows

package volume

import (
	"encoding/base64"
	"strings"

	"remount.dev/remount/internal/artifact"
)

func sourcePathComponents(tenant, id string) (string, string) {
	return "tenant-" + base64.RawURLEncoding.EncodeToString([]byte(tenant)),
		strings.TrimPrefix(id, artifact.Prefix)
}
