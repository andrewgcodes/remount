//go:build !linux && !darwin

package workspace

func totalMemMiB() int { return 0 }
