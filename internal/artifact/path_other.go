//go:build !windows

package artifact

func validatePlatformArchiveName(string) error { return nil }
