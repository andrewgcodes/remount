//go:build windows

package encrypted

import "os"

func validateMasterKeyPermissions(os.FileInfo) error { return nil }
