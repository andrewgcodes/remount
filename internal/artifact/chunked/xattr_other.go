//go:build !linux && !darwin

package chunked

import (
	"errors"
	"os"
)

func readXattrs(_ *os.File, _ Limits) ([]Xattr, error) { return nil, nil }

func applyXattrs(_ *os.File, attrs []Xattr) error {
	if len(attrs) != 0 {
		return errors.New("chunked artifact: extended attributes are unsupported on this platform")
	}
	return nil
}

func applyXattrsPath(_ string, attrs []Xattr) error {
	if len(attrs) != 0 {
		return errors.New("chunked artifact: extended attributes are unsupported on this platform")
	}
	return nil
}
