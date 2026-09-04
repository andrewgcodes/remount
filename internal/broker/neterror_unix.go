//go:build !windows

package broker

func isPlatformConnectionRefused(error) bool { return false }

func isPlatformConnectionReset(error) bool { return false }
