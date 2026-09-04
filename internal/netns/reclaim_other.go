//go:build !linux

package netns

import "context"

// ReclaimOrphans has nothing to reclaim off Linux, where the boundary this
// package builds cannot exist in the first place.
func ReclaimOrphans(context.Context) (int, error) { return 0, nil }
