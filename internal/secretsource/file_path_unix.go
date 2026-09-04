//go:build !windows

package secretsource

import (
	"errors"
	"path/filepath"
)

func fileURLPath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", errors.New("secretsource: file source must be an absolute file URL")
	}
	return path, nil
}
