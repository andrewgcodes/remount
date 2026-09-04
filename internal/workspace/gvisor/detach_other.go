//go:build !linux

package gvisor

// detachMount is a no-op off Linux. The gVisor backend refuses to start on any
// other OS, so nothing reaches this; it exists so the package still builds for
// the cross-compilation the release matrix does.
func detachMount(string) error { return nil }
