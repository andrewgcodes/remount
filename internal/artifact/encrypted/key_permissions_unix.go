//go:build !windows

package encrypted

import (
	"errors"
	"os"
)

func validateMasterKeyPermissions(info os.FileInfo) error {
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("master key is accessible to group or world")
	}
	return nil
}
