package tools

import (
	"context"
	"strings"
	"testing"
	"time"

	workspace "swarm/internal/runtime/workspace"
)

func TestNativeWorkspaceCommandFailsClosedForHostBackend(t *testing.T) {
	exec := &Executor{}
	_, _, exitCode, err := exec.runWorkspaceCommand(context.Background(), &workspace.Target{
		Workdir: t.TempDir(),
		Backend: workspace.BackendHost,
	}, time.Second, "", "sh", "-lc", "true")
	if err == nil || !strings.Contains(err.Error(), "host workspace backend does not support native tool execution yet") {
		t.Fatalf("runWorkspaceCommand error = %v, want host backend fail-closed error", err)
	}
	if exitCode != -1 {
		t.Fatalf("exit code = %d, want -1 for fail-closed host backend", exitCode)
	}
}
