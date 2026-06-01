package workspace

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecutionTargetDistinguishesDockerHostAndEmptyContainer(t *testing.T) {
	docker := (&Target{Backend: BackendDocker, Container: "swarm-agent", Workdir: "/workspace"}).ExecutionTarget()
	if docker.Mode != ExecutionModeDockerContainer || !docker.Supports(ExecutionCapabilityNativeCommand) || !docker.Supports(ExecutionCapabilityClaudeCLI) {
		t.Fatalf("docker execution target = %#v, want container execution capabilities", docker)
	}

	host := (&Target{Backend: BackendHost, Workdir: t.TempDir()}).ExecutionTarget()
	if host.Mode != ExecutionModeHostLocal || !host.Supports(ExecutionCapabilityFileRead) || !host.Supports(ExecutionCapabilityFileWrite) {
		t.Fatalf("host execution target = %#v, want explicit host file capabilities", host)
	}
	if host.Supports(ExecutionCapabilityNativeCommand) || host.Supports(ExecutionCapabilityToolResultRelay) || host.Supports(ExecutionCapabilityClaudeCLI) {
		t.Fatalf("host execution target = %#v, want command/relay/claude unsupported", host)
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

func TestExecutionTargetHostBackingPathAuthority(t *testing.T) {
	workspaceDir := t.TempDir()
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	target := (&Target{
		Backend: BackendHost,
		Workdir: workspaceDir,
		Mounts: []ExecutionMount{
			{LogicalPath: LogicalWorkspaceMount, HostPath: workspaceDir, Access: MountAccessReadWrite},
			{LogicalPath: LogicalDataMount, HostPath: dataDir, Access: MountAccessReadOnly},
			{LogicalPath: LogicalContractsMount, HostPath: contractsDir, Access: MountAccessReadOnly},
		},
	}).ExecutionTarget()

	workspacePath, err := target.ResolveHostPath("drafts/out.txt", PathAccessWrite)
	if err != nil {
		t.Fatalf("ResolveHostPath workspace write: %v", err)
	}
	wantWorkspaceHostPath, err := canonicalPathForOverlap(filepath.Join(workspaceDir, "drafts", "out.txt"), "test workspace path")
	if err != nil {
		t.Fatalf("canonical workspace path: %v", err)
	}
	if workspacePath.LogicalPath != "/workspace/drafts/out.txt" || workspacePath.HostPath != wantWorkspaceHostPath {
		t.Fatalf("workspace host path = %#v", workspacePath)
	}

	dataPath, err := target.ResolveHostPath("/data/ref.txt", PathAccessRead)
	if err != nil {
		t.Fatalf("ResolveHostPath data read: %v", err)
	}
	wantDataHostPath, err := canonicalPathForOverlap(filepath.Join(dataDir, "ref.txt"), "test data path")
	if err != nil {
		t.Fatalf("canonical data path: %v", err)
	}
	if dataPath.LogicalPath != "/data/ref.txt" || dataPath.HostPath != wantDataHostPath {
		t.Fatalf("data host path = %#v", dataPath)
	}

	if _, err := target.ResolveHostPath("/data/ref.txt", PathAccessWrite); err == nil || !strings.Contains(err.Error(), "outside the writable workspace") {
		t.Fatalf("data write error = %v, want read-only rejection", err)
	}
	if _, err := target.ResolveHostPath(workspaceDir, PathAccessRead); err == nil || !strings.Contains(err.Error(), "outside the allowed workspace mounts") {
		t.Fatalf("raw host path error = %v, want logical path rejection", err)
	}

	if err := os.WriteFile(filepath.Join(dataDir, "ref.txt"), []byte("outside"), 0o644); err != nil {
		t.Fatalf("write data ref: %v", err)
	}
	if err := os.Symlink(dataDir, filepath.Join(workspaceDir, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := target.ResolveHostPath("/workspace/link/ref.txt", PathAccessRead); err == nil || !strings.Contains(err.Error(), "escapes /workspace") {
		t.Fatalf("symlink escape error = %v, want escape rejection", err)
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
