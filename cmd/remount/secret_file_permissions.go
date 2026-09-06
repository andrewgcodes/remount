package main

import (
	"os"

	"remount.dev/remount/internal/privatefile"
)

func secureSecretFile(path string) error {
	return privatefile.SecureFile(path)
}

func validateSecretFilePermissions(path string, info os.FileInfo) error {
	return privatefile.ValidateFile(path, info)
}
