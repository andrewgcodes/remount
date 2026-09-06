package encrypted

import (
	"os"

	"remount.dev/remount/internal/privatefile"
)

func validateMasterKeyPermissions(path string, info os.FileInfo) error {
	return privatefile.ValidateFile(path, info)
}
