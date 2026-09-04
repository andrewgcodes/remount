//go:build !windows

package fsops

func validatePlatformPath(string) error { return nil }
