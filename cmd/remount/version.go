package main

import "runtime/debug"

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		version = resolveVersion(version, info)
	}
}

func resolveVersion(linked string, info *debug.BuildInfo) string {
	if linked != "dev" || info == nil || info.Main.Version == "" || info.Main.Version == "(devel)" {
		return linked
	}
	return info.Main.Version
}
