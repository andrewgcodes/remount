//go:build windows

package node

import (
	"errors"
	"testing"

	"remount.dev/remount/internal/proto"
)

func TestRecipeACPProgramRefusesWindowsProcessWorkspace(t *testing.T) {
	if _, err := recipeACPProgram("process", `C:\agent\launch.cmd`); err == nil {
		t.Fatal("recipeACPProgram accepted a POSIX-shell launcher in a Windows process workspace")
	} else {
		var protocolError *proto.Error
		if !errors.As(err, &protocolError) || protocolError.Code != proto.CodeUnsupported {
			t.Fatalf("recipeACPProgram error = %v", err)
		}
	}
}
