//go:build linux || darwin

package chunked

import (
	"errors"
	"os"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

func readXattrs(file *os.File, limits Limits) ([]Xattr, error) {
	size, err := unix.Flistxattr(int(file.Fd()), nil)
	if err != nil {
		if errors.Is(err, unix.ENOTSUP) {
			return nil, nil
		}
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}
	if size > limits.MaxXattrsPerEntry*(limits.MaxPathBytes+1) {
		return nil, errors.New("chunked artifact: extended attribute names exceed limit")
	}
	namesBuf := make([]byte, size)
	n, err := unix.Flistxattr(int(file.Fd()), namesBuf)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, name := range strings.Split(string(namesBuf[:n]), "\x00") {
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) > limits.MaxXattrsPerEntry {
		return nil, errors.New("chunked artifact: too many extended attributes")
	}
	out := make([]Xattr, 0, len(names))
	for _, name := range names {
		size, err := unix.Fgetxattr(int(file.Fd()), name, nil)
		if err != nil {
			return nil, err
		}
		if size > limits.MaxXattrBytes {
			return nil, errors.New("chunked artifact: extended attribute exceeds limit")
		}
		value := make([]byte, size)
		n, err := unix.Fgetxattr(int(file.Fd()), name, value)
		if err != nil {
			return nil, err
		}
		out = append(out, Xattr{Name: name, Value: value[:n]})
	}
	return out, nil
}

func applyXattrs(file *os.File, attrs []Xattr) error {
	for _, attr := range attrs {
		if err := unix.Fsetxattr(int(file.Fd()), attr.Name, attr.Value, 0); err != nil {
			return err
		}
	}
	return nil
}

func applyXattrsPath(path string, attrs []Xattr) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	applyErr := applyXattrs(file, attrs)
	closeErr := file.Close()
	return errors.Join(applyErr, closeErr)
}
