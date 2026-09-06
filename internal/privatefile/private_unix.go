//go:build !windows

package privatefile

import (
	"fmt"
	"os"
)

// SecureFile restricts path to the current Unix account.
func SecureFile(path string) error {
	return os.Chmod(path, 0o600)
}

// SecureDirectory restricts path to the current Unix account.
func SecureDirectory(path string) error {
	return os.Chmod(path, 0o700)
}

// ValidateFile rejects a file readable by another Unix account.
func ValidateFile(path string, info os.FileInfo) error {
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf("%s is mode %#o: a private file must not be group- or world-accessible", path, perm)
	}
	return nil
}
