//go:build windows

package artifact

import (
	"fmt"
	"strings"
)

func validatePlatformArchiveName(name string) error {
	for _, part := range strings.Split(name, "/") {
		if strings.Contains(part, ":") {
			return fmt.Errorf("artifact: entry %q contains a Windows alternate data stream", name)
		}
		if strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") {
			return fmt.Errorf("artifact: entry %q has a Windows-ambiguous trailing character", name)
		}
		base, _, _ := strings.Cut(part, ".")
		switch strings.ToUpper(base) {
		case "CON", "PRN", "AUX", "NUL", "CLOCK$",
			"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
			"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
			return fmt.Errorf("artifact: entry %q contains a reserved Windows device name", name)
		}
	}
	return nil
}
