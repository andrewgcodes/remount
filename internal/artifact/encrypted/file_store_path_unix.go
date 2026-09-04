//go:build !windows

package encrypted

func physicalObjectKey(key string) string { return key }

func logicalObjectKey(key string) string { return key }
