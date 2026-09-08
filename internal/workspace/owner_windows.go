//go:build windows

package workspace

import "io/fs"

// Windows has no uid ownership to verify; reown is never needed there.
func fileOwnedBy(fs.FileInfo, int) bool { return true }
