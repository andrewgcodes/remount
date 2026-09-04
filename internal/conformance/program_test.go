package conformance

import "testing"

func TestWorkspaceProgramsFollowBackendEnvironment(t *testing.T) {
	process := &Target{Backend: "process", Environment: Environment{OS: "windows"}}
	if got := echoProgram(process.workspaceOS(), "native")[0]; got != "cmd.exe" {
		t.Fatalf("Windows process program = %q", got)
	}

	docker := &Target{Backend: "docker", Environment: Environment{OS: "windows"}}
	if got := echoProgram(docker.workspaceOS(), "container")[0]; got != "/bin/echo" {
		t.Fatalf("Linux Docker program = %q", got)
	}
}
