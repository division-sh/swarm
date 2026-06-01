package workspace

import (
	"strings"
	"testing"
)

func TestExecutionTargetDistinguishesDockerHostAndEmptyContainer(t *testing.T) {
	docker := (&Target{Backend: BackendDocker, Container: "swarm-agent", Workdir: "/workspace"}).ExecutionTarget()
	if docker.Mode != ExecutionModeDockerContainer || !docker.Supports(ExecutionCapabilityNativeCommand) || !docker.Supports(ExecutionCapabilityClaudeCLI) {
		t.Fatalf("docker execution target = %#v, want container execution capabilities", docker)
	}

	host := (&Target{Backend: BackendHost, Workdir: t.TempDir()}).ExecutionTarget()
	if host.Mode != ExecutionModeHostLocal || host.Supports(ExecutionCapabilityNativeCommand) || host.Supports(ExecutionCapabilityClaudeCLI) {
		t.Fatalf("host execution target = %#v, want explicit unsupported host-local target", host)
	}
	if err := host.Require(ExecutionCapabilityNativeCommand); err == nil || !strings.Contains(err.Error(), "host workspace backend does not support native tool execution yet") {
		t.Fatalf("host native command error = %v, want fail-closed diagnostic", err)
	}

	empty := (&Target{Workdir: t.TempDir()}).ExecutionTarget()
	if empty.Mode != ExecutionModeUnsupported || empty.Supports(ExecutionCapabilityNativeCommand) {
		t.Fatalf("empty-container execution target = %#v, want unsupported", empty)
	}
}

func TestExecutionTargetLogicalPathAuthority(t *testing.T) {
	target := (&Target{
		Backend: BackendHost,
		Workdir: t.TempDir(),
		Mounts:  []ExecutionMount{{LogicalPath: LogicalWorkspaceMount, Access: MountAccessReadWrite}, {LogicalPath: LogicalDataMount, Access: MountAccessReadOnly}, {LogicalPath: LogicalContractsMount, Access: MountAccessReadOnly}},
	}).ExecutionTarget()

	if got, err := target.ResolvePath("draft.txt", PathAccessWrite); err != nil || got != "/workspace/draft.txt" {
		t.Fatalf("workspace write path = %q err=%v, want /workspace/draft.txt", got, err)
	}
	if got, err := target.ResolvePath("/data/corpus.jsonl", PathAccessRead); err != nil || got != "/data/corpus.jsonl" {
		t.Fatalf("data read path = %q err=%v, want /data/corpus.jsonl", got, err)
	}
	if _, err := target.ResolvePath("/data/corpus.jsonl", PathAccessWrite); err == nil || !strings.Contains(err.Error(), "outside the writable workspace") {
		t.Fatalf("data write error = %v, want read-only rejection", err)
	}
	if _, err := target.ResolvePath("/tmp/escape", PathAccessWrite); err == nil || !strings.Contains(err.Error(), "outside the writable workspace") {
		t.Fatalf("outside write error = %v, want fail-closed rejection", err)
	}
	if _, err := target.ResolvePath(t.TempDir(), PathAccessRead); err == nil || !strings.Contains(err.Error(), "outside the allowed workspace mounts") {
		t.Fatalf("raw host read error = %v, want raw host path rejection", err)
	}
}

func TestExecutionTargetWorkspacePathStaysLogical(t *testing.T) {
	target := (&Target{Backend: BackendHost, Workdir: t.TempDir()}).ExecutionTarget()
	got, err := target.WorkspacePath(".swarm/tool-results/session/result.json")
	if err != nil {
		t.Fatalf("WorkspacePath: %v", err)
	}
	if got != "/workspace/.swarm/tool-results/session/result.json" {
		t.Fatalf("workspace path = %q, want logical /workspace path", got)
	}
	if _, err := target.WorkspacePath("/data/result.json"); err == nil || !strings.Contains(err.Error(), "outside /workspace") {
		t.Fatalf("WorkspacePath /data error = %v, want workspace-only rejection", err)
	}
}
