package secretsource

import (
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
)

func parseFileSource(source string) (string, error) {
	u, err := url.Parse(source)
	if err != nil || u.Scheme != "file" || u.Host != "" || u.RawQuery != "" || u.Fragment != "" || !filepath.IsAbs(u.Path) {
		return "", errors.New("secretsource: file source must be an absolute file URL")
	}
	return filepath.Clean(u.Path), nil
}

func readSecretFile(path string, maxBytes int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, sourceError("file", "read")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, sourceError("file", "read")
	}
	value, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, sourceError("file", "read")
	}
	if int64(len(value)) > maxBytes {
		zero(value)
		return nil, errors.New("secretsource: file value exceeds byte limit")
	}
	if len(value) == 0 {
		return nil, errors.New("secretsource: file source is empty")
	}
	return value, nil
}
