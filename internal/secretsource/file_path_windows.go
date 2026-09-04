//go:build windows

package secretsource

import (
	"errors"
	"path/filepath"
)

func fileURLPath(path string) (string, error) {
	if len(path) < 4 || path[0] != '/' || path[2] != ':' || path[3] != '/' {
		return "", errors.New("secretsource: file source must be an absolute file URL")
	}
	path = filepath.FromSlash(path[1:])
	if !filepath.IsAbs(path) {
		return "", errors.New("secretsource: file source must be an absolute file URL")
	}
	return path, nil
}
