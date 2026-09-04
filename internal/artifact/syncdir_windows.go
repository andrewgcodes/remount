//go:build windows

package artifact

import "os"

func syncDir(string) error { return nil }

func syncRootPath(*os.Root, string) error { return nil }
