package tools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestNativeWorkspaceCommandRunsInExplicitHostBackendWorkdir(t *testing.T) {
	workspaceDir := t.TempDir()
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	exec := &Executor{}
	stdout, stderr, exitCode, err := exec.runWorkspaceCommand(unmanagedToolTestContext(), &workspace.Target{
		Workdir: workspaceDir,
		Backend: workspace.BackendHost,
		Mounts: []workspace.ExecutionMount{
			{LogicalPath: workspace.LogicalWorkspaceMount, HostPath: workspaceDir, Access: workspace.MountAccessReadWrite},
			{LogicalPath: workspace.LogicalDataMount, HostPath: dataDir, Access: workspace.MountAccessReadOnly},
			{LogicalPath: workspace.LogicalContractsMount, HostPath: contractsDir, Access: workspace.MountAccessReadOnly},
		},
	}, "native_bash", time.Second, "", "sh", "-lc", "mkdir -p nested && printf host-ok > nested/out.txt && printf done")
	if err != nil {
		t.Fatalf("runWorkspaceCommand host error = %v stderr=%s", err, stderr)
	}
	if exitCode != 0 {
		t.Fatalf("exit code = %d stdout=%s stderr=%s, want 0", exitCode, stdout, stderr)
	}
	if string(stdout) != "done" {
		t.Fatalf("stdout = %q, want done", stdout)
	}
	data, err := os.ReadFile(filepath.Join(workspaceDir, "nested", "out.txt"))
	if err != nil {
		t.Fatalf("read host command output: %v", err)
	}
	if string(data) != "host-ok" {
		t.Fatalf("host command output file = %q, want host-ok", data)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "nested", "out.txt")); !os.IsNotExist(err) {
		t.Fatalf("host command wrote outside workspace dataDir err=%v", err)
	}
}

func TestNativeWorkspaceCommandFailsClosedForHostTargetWithoutBackingPath(t *testing.T) {
	exec := &Executor{}
	_, _, exitCode, err := exec.runWorkspaceCommand(unmanagedToolTestContext(), &workspace.Target{
		Workdir: t.TempDir(),
		Backend: workspace.BackendHost,
	}, "native_bash", time.Second, "", "sh", "-lc", "true")
	if err == nil || !strings.Contains(err.Error(), "host native command workspace path is unavailable") {
		t.Fatalf("runWorkspaceCommand error = %v, want host backing path fail-closed error", err)
	}
	if exitCode != -1 {
		t.Fatalf("exit code = %d, want -1 for fail-closed host backing path", exitCode)
	}
}

func TestNativeWorkspaceCommandDoesNotFallbackFromDockerToHost(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "fallback-marker")
	missingDocker := filepath.Join(t.TempDir(), "missing-docker")
	exec := &Executor{cfg: &config.Config{Workspace: config.WorkspaceConfig{DockerBin: missingDocker}}}
	_, _, exitCode, err := exec.runWorkspaceCommand(unmanagedToolTestContext(), &workspace.Target{
		Backend:   workspace.BackendDocker,
		Container: "swarm-agent",
		Workdir:   workspace.LogicalWorkspaceMount,
	}, "native_bash", time.Second, "", "sh", "-lc", "printf fallback > "+shellQuote(marker))
	if err == nil {
		t.Fatal("runWorkspaceCommand docker with missing binary succeeded, want fail closed")
	}
	if exitCode != -1 {
		t.Fatalf("exit code = %d, want -1 for missing Docker binary", exitCode)
	}
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Fatalf("docker failure fell back to host command; marker stat err=%v", statErr)
	}
}

func TestNativeFileToolsUseHostExecutionTargetWithoutShell(t *testing.T) {
	workspaceDir := t.TempDir()
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspaceDir, "input.txt"), []byte("workspace content"), 0o644); err != nil {
		t.Fatalf("write workspace input: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "ref.txt"), []byte("data content"), 0o644); err != nil {
		t.Fatalf("write data ref: %v", err)
	}
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("contracts content"), 0o644); err != nil {
		t.Fatalf("write contracts package: %v", err)
	}
	exec := &Executor{
		workspaces: relayWorkspaceResolverStub{
			target: &workspace.Target{
				Backend: workspace.BackendHost,
				Workdir: workspaceDir,
				Mounts: []workspace.ExecutionMount{
					{LogicalPath: workspace.LogicalWorkspaceMount, HostPath: workspaceDir, Access: workspace.MountAccessReadWrite},
					{LogicalPath: workspace.LogicalDataMount, HostPath: dataDir, Access: workspace.MountAccessReadOnly},
					{LogicalPath: workspace.LogicalContractsMount, HostPath: contractsDir, Access: workspace.MountAccessReadOnly},
				},
			},
		},
		execWorkspaceFn: func(context.Context, workspace.ExecutionTarget, time.Duration, string, ...string) ([]byte, []byte, int, error) {
			t.Fatalf("host native file tools must not shell through workspace command execution")
			return nil, nil, 0, nil
		},
	}
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ID:          "writer",
		NativeTools: models.NativeToolConfig{FileIO: true},
	})

	readWorkspace, err := exec.execNativeReadFile(ctx, models.AgentConfig{ID: "writer"}, map[string]any{"path": "/workspace/input.txt"})
	if err != nil {
		t.Fatalf("execNativeReadFile workspace: %v", err)
	}
	if got := readWorkspace.(map[string]any)["content"]; got != "workspace content" {
		t.Fatalf("workspace read content = %#v", got)
	}
	readData, err := exec.execNativeReadFile(ctx, models.AgentConfig{ID: "writer"}, map[string]any{"path": "/data/ref.txt"})
	if err != nil {
		t.Fatalf("execNativeReadFile data: %v", err)
	}
	if got := readData.(map[string]any)["content"]; got != "data content" {
		t.Fatalf("data read content = %#v", got)
	}
	readContracts, err := exec.execNativeReadFile(ctx, models.AgentConfig{ID: "writer"}, map[string]any{"path": "/opt/swarm/contracts/package.yaml"})
	if err != nil {
		t.Fatalf("execNativeReadFile contracts: %v", err)
	}
	if got := readContracts.(map[string]any)["content"]; got != "contracts content" {
		t.Fatalf("contracts read content = %#v", got)
	}

	written, err := exec.execNativeWriteFile(ctx, models.AgentConfig{ID: "writer"}, map[string]any{"path": "/workspace/nested/output.txt", "content": "hello"})
	if err != nil {
		t.Fatalf("execNativeWriteFile workspace: %v", err)
	}
	if got := written.(map[string]any)["bytes_written"]; got != len([]byte("hello")) {
		t.Fatalf("write bytes = %#v", got)
	}
	if data, err := os.ReadFile(filepath.Join(workspaceDir, "nested", "output.txt")); err != nil || string(data) != "hello" {
		t.Fatalf("written workspace file = %q err=%v", data, err)
	}

	for _, forbidden := range []string{"/data/out.txt", "/opt/swarm/contracts/out.txt", "/tmp/out.txt", workspaceDir} {
		_, err := exec.execNativeWriteFile(ctx, models.AgentConfig{ID: "writer"}, map[string]any{"path": forbidden, "content": "nope"})
		if err == nil {
			t.Fatalf("execNativeWriteFile(%q) succeeded, want fail closed", forbidden)
		}
		if strings.Contains(err.Error(), workspaceDir) || strings.Contains(err.Error(), dataDir) || strings.Contains(err.Error(), contractsDir) {
			t.Fatalf("execNativeWriteFile(%q) leaked host path in error: %v", forbidden, err)
		}
	}

	if err := os.Symlink(dataDir, filepath.Join(workspaceDir, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	_, err = exec.execNativeReadFile(ctx, models.AgentConfig{ID: "writer"}, map[string]any{"path": "/workspace/link/ref.txt"})
	if err == nil || !strings.Contains(err.Error(), "escapes /workspace") {
		t.Fatalf("execNativeReadFile symlink error = %v, want escape rejection", err)
	}
}

func TestExecutorHostFileToolsUseHostManagerSupportedSurfaceWithoutDocker(t *testing.T) {
	ctx := unmanagedToolTestContext()
	workspaceRoot := filepath.Join(t.TempDir(), "host-workspaces")
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "ref.txt"), []byte("data content"), 0o644); err != nil {
		t.Fatalf("write data ref: %v", err)
	}
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("contracts content"), 0o644); err != nil {
		t.Fatalf("write contracts package: %v", err)
	}

	manager := workspace.NewHostManager(nil)
	manager.SetConfig(workspace.HostConfig{
		WorkspaceRoot:       workspaceRoot,
		SharedDataSource:    dataDir,
		DataMountPoint:      workspace.LogicalDataMount,
		ContractsSource:     contractsDir,
		ContractsMountPoint: workspace.LogicalContractsMount,
	})
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	if err := manager.ValidateSource(ctx, source); err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}
	if err := manager.EnsurePrereqs(ctx); err != nil {
		t.Fatalf("EnsurePrereqs: %v", err)
	}

	actor := models.AgentConfig{
		ID:          "host-file-agent",
		NativeTools: models.NativeToolConfig{FileIO: true, Bash: true},
	}
	target, err := manager.ResolveWorkspace(ctx, actor)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	if target == nil || !target.HostBackend() {
		t.Fatalf("resolved workspace target = %#v, want host backend", target)
	}
	if err := os.WriteFile(filepath.Join(target.Workdir, "input.txt"), []byte("workspace content"), 0o644); err != nil {
		t.Fatalf("write workspace input: %v", err)
	}

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		ModelRuntime:      nativeCapabilityRuntimeStub{},
		WorkspaceResolver: manager,
	})
	exec.execWorkspaceFn = func(context.Context, workspace.ExecutionTarget, time.Duration, string, ...string) ([]byte, []byte, int, error) {
		t.Fatalf("host file tools must use direct platform file operations, not workspace command execution")
		return nil, nil, 0, nil
	}
	actorCtx := models.WithActor(ctx, actor)

	readWorkspace, err := exec.Execute(actorCtx, "read_file", map[string]any{"path": "/workspace/input.txt"})
	if err != nil {
		t.Fatalf("Execute read_file workspace: %v", err)
	}
	if got := readWorkspace.(map[string]any)["content"]; got != "workspace content" {
		t.Fatalf("workspace content = %#v", got)
	}
	readData, err := exec.Execute(actorCtx, "read_file", map[string]any{"path": "/data/ref.txt"})
	if err != nil {
		t.Fatalf("Execute read_file data: %v", err)
	}
	if got := readData.(map[string]any)["content"]; got != "data content" {
		t.Fatalf("data content = %#v", got)
	}
	readContracts, err := exec.Execute(actorCtx, "read_file", map[string]any{"path": "/opt/swarm/contracts/package.yaml"})
	if err != nil {
		t.Fatalf("Execute read_file contracts: %v", err)
	}
	if got := readContracts.(map[string]any)["content"]; got != "contracts content" {
		t.Fatalf("contracts content = %#v", got)
	}
	written, err := exec.Execute(actorCtx, "write_file", map[string]any{"path": "/workspace/out/result.txt", "content": "hello"})
	if err != nil {
		t.Fatalf("Execute write_file workspace: %v", err)
	}
	if got := written.(map[string]any)["bytes_written"]; got != len([]byte("hello")) {
		t.Fatalf("bytes_written = %#v", got)
	}
	if data, err := os.ReadFile(filepath.Join(target.Workdir, "out", "result.txt")); err != nil || string(data) != "hello" {
		t.Fatalf("written host workspace file = %q err=%v", data, err)
	}

	for _, forbidden := range []string{
		"/data/out.txt",
		"/opt/swarm/contracts/out.txt",
		"/tmp/out.txt",
		filepath.Join(target.Workdir, "out", "result.txt"),
		"../escape.txt",
	} {
		_, err := exec.Execute(actorCtx, "write_file", map[string]any{"path": forbidden, "content": "nope"})
		if err == nil {
			t.Fatalf("Execute write_file(%q) succeeded, want fail closed", forbidden)
		}
		if strings.Contains(err.Error(), target.Workdir) || strings.Contains(err.Error(), dataDir) || strings.Contains(err.Error(), contractsDir) {
			t.Fatalf("Execute write_file(%q) leaked host path in error: %v", forbidden, err)
		}
	}

	if err := os.Symlink(dataDir, filepath.Join(target.Workdir, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	_, err = exec.Execute(actorCtx, "read_file", map[string]any{"path": "/workspace/link/ref.txt"})
	if err == nil || !strings.Contains(err.Error(), "escapes /workspace") {
		t.Fatalf("Execute read_file symlink error = %v, want escape rejection", err)
	}
}

func TestExecutorHostNativeBashUsesExplicitHostManagerTarget(t *testing.T) {
	ctx := unmanagedToolTestContext()
	workspaceRoot := filepath.Join(t.TempDir(), "host-workspaces")
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("contracts content"), 0o644); err != nil {
		t.Fatalf("write contracts package: %v", err)
	}

	manager := workspace.NewHostManager(nil)
	manager.SetConfig(workspace.HostConfig{
		WorkspaceRoot:       workspaceRoot,
		SharedDataSource:    dataDir,
		DataMountPoint:      workspace.LogicalDataMount,
		ContractsSource:     contractsDir,
		ContractsMountPoint: workspace.LogicalContractsMount,
	})
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	if err := manager.ValidateSource(ctx, source); err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}
	if err := manager.EnsurePrereqs(ctx); err != nil {
		t.Fatalf("EnsurePrereqs: %v", err)
	}

	actor := models.AgentConfig{
		ID:          "host-bash-agent",
		NativeTools: models.NativeToolConfig{Bash: true},
	}
	target, err := manager.ResolveWorkspace(ctx, actor)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		ModelRuntime:      nativeCapabilityRuntimeStub{},
		WorkspaceResolver: manager,
	})
	actorCtx := models.WithActor(ctx, actor)

	out, err := exec.Execute(actorCtx, "bash", map[string]any{
		"command": "mkdir -p cmd && printf host-bash > cmd/out.txt && printf ok",
	})
	if err != nil {
		t.Fatalf("Execute bash host: %v", err)
	}
	result := out.(map[string]any)
	if got := result["stdout"]; got != "ok" {
		t.Fatalf("host bash stdout = %#v, want ok", got)
	}
	if got := result["exit_code"]; got != 0 {
		t.Fatalf("host bash exit_code = %#v, want 0", got)
	}
	if data, err := os.ReadFile(filepath.Join(target.Workdir, "cmd", "out.txt")); err != nil || string(data) != "host-bash" {
		t.Fatalf("host bash workspace file = %q err=%v", data, err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "cmd", "out.txt")); !os.IsNotExist(err) {
		t.Fatalf("host bash wrote outside workspace dataDir err=%v", err)
	}
}

func TestNativeFileToolsKeepDockerWorkspaceCommandExecution(t *testing.T) {
	exec := &Executor{
		workspaces: relayWorkspaceResolverStub{
			target: &workspace.Target{
				Backend:   workspace.BackendDocker,
				Container: "swarm-agent",
				Workdir:   workspace.LogicalWorkspaceMount,
			},
		},
	}
	var calls []string
	exec.execWorkspaceFn = func(_ context.Context, execTarget workspace.ExecutionTarget, _ time.Duration, stdin string, args ...string) ([]byte, []byte, int, error) {
		if execTarget.Mode != workspace.ExecutionModeDockerContainer {
			t.Fatalf("exec target mode = %s, want docker_container", execTarget.Mode)
		}
		if execTarget.Container != "swarm-agent" {
			t.Fatalf("exec target container = %q, want swarm-agent", execTarget.Container)
		}
		if len(args) != 5 || args[0] != "sh" || args[1] != "-lc" {
			t.Fatalf("workspace command args = %#v, want shell command with argv path", args)
		}
		calls = append(calls, strings.Join(args, "\x00"))
		switch args[3] {
		case "swarm-read-file":
			if args[4] != "/workspace/input.txt" {
				t.Fatalf("read path = %q, want /workspace/input.txt", args[4])
			}
			return []byte("docker content"), nil, 0, nil
		case "swarm-write-file":
			if args[4] != "/workspace/out/result.txt" {
				t.Fatalf("write path = %q, want /workspace/out/result.txt", args[4])
			}
			if stdin != "hello" {
				t.Fatalf("write stdin = %q, want hello", stdin)
			}
			return nil, nil, 0, nil
		default:
			t.Fatalf("unexpected workspace command label: %#v", args)
			return nil, nil, 1, nil
		}
	}
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ID:          "docker-file-agent",
		NativeTools: models.NativeToolConfig{FileIO: true},
	})

	read, err := exec.execNativeReadFile(ctx, models.AgentConfig{ID: "docker-file-agent"}, map[string]any{"path": "/workspace/input.txt"})
	if err != nil {
		t.Fatalf("execNativeReadFile docker: %v", err)
	}
	if got := read.(map[string]any)["content"]; got != "docker content" {
		t.Fatalf("docker read content = %#v", got)
	}
	written, err := exec.execNativeWriteFile(ctx, models.AgentConfig{ID: "docker-file-agent"}, map[string]any{"path": "out/result.txt", "content": "hello"})
	if err != nil {
		t.Fatalf("execNativeWriteFile docker: %v", err)
	}
	if got := written.(map[string]any)["bytes_written"]; got != len([]byte("hello")) {
		t.Fatalf("docker write bytes = %#v", got)
	}
	if len(calls) != 2 {
		t.Fatalf("docker command calls = %#v, want two command executions", calls)
	}
}

func TestNativeFallbackToolSurfaceConsumesWorkspaceExecutionTarget(t *testing.T) {
	exec := &Executor{
		modelRuntime: nativeCapabilityRuntimeStub{},
		workspaces: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
	}
	actor := models.AgentConfig{
		ID: "host-agent",
		NativeTools: models.NativeToolConfig{
			Bash:   true,
			FileIO: true,
		},
	}

	defs := exec.ToolDefinitionsForActorInContext(unmanagedToolTestContext(), actor)
	for _, allowed := range []string{"bash", "read_file", "write_file"} {
		if !containsToolDefinition(defs, allowed) {
			t.Fatalf("context definitions missing %q: %#v", allowed, defs)
		}
	}

	caps := exec.ToolCapabilitiesForActorInContext(unmanagedToolTestContext(), actor, []string{"bash", "read_file", "write_file", "web_search"}, nil)
	for _, allowed := range []string{"bash", "read_file", "write_file"} {
		cap := caps.ByName[allowed]
		if !cap.Visible || !cap.Callable || cap.DenialReason != "" {
			t.Fatalf("capability %s = %#v, want visible/callable host native operation", allowed, cap)
		}
	}
	web := caps.ByName["web_search"]
	if web.Visible || web.Callable {
		t.Fatalf("web_search capability = %#v, want hidden because native_tools.web_search is not enabled", web)
	}
}

func TestNativeFallbackToolSurfaceRejectsStrictProviderNativeRuntime(t *testing.T) {
	workspaceDir := t.TempDir()
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		ModelRuntime: nativeCapabilityRuntimeStub{
			caps: llm.NativeToolCapabilities{
				Bash:      true,
				FileIO:    true,
				WebSearch: true,
			},
			strict: true,
		},
		WorkspaceResolver: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: workspaceDir},
		},
	})
	actor := models.AgentConfig{
		ID: "strict-provider-agent",
		NativeTools: models.NativeToolConfig{
			Bash:      true,
			FileIO:    true,
			WebSearch: true,
		},
	}

	defs := exec.ToolDefinitionsForActorInContext(unmanagedToolTestContext(), actor)
	for _, denied := range []string{"bash", "read_file", "write_file", "web_search"} {
		if containsToolDefinition(defs, denied) {
			t.Fatalf("strict provider-native runtime exposed platform fallback definition %q: %#v", denied, defs)
		}
		if _, ok, err := exec.resolveRegisteredTool(actor, denied); err != nil || ok {
			t.Fatalf("resolveRegisteredTool(%s) = ok:%v err:%v, want denied without error", denied, ok, err)
		}
	}

	caps := exec.ToolCapabilitiesForActorInContext(unmanagedToolTestContext(), actor, []string{"bash", "read_file", "write_file", "web_search"}, nil)
	for _, denied := range []string{"bash", "read_file", "write_file", "web_search"} {
		cap := caps.ByName[denied]
		if cap.Visible || cap.Callable {
			t.Fatalf("strict provider-native capability %s = %#v, want hidden/non-callable fallback surface", denied, cap)
		}
		if !strings.Contains(cap.DenialReason, nativeToolProviderOnlyFallbackDeny) {
			t.Fatalf("strict provider-native capability %s denial = %q, want provider-native-only denial", denied, cap.DenialReason)
		}
	}

	exec.execWorkspaceFn = func(context.Context, workspace.ExecutionTarget, time.Duration, string, ...string) ([]byte, []byte, int, error) {
		t.Fatal("strict provider-native runtime must not execute platform fallback workspace tools")
		return nil, nil, 0, nil
	}
	_, err := exec.Execute(models.WithActor(unmanagedToolTestContext(), actor), "read_file", map[string]any{"path": "/workspace/missing.txt"})
	if err == nil || !strings.Contains(err.Error(), "unsupported runtime tool") {
		t.Fatalf("Execute(read_file) error = %v, want fallback execution denial", err)
	}
}

func TestNativeWorkspaceCommandRequiresActorBashAuthorization(t *testing.T) {
	workspaceDir := t.TempDir()
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkspaceResolver: relayWorkspaceResolverStub{
			target: &workspace.Target{
				Backend: workspace.BackendHost,
				Workdir: workspaceDir,
				Mounts: []workspace.ExecutionMount{
					{LogicalPath: workspace.LogicalWorkspaceMount, HostPath: workspaceDir, Access: workspace.MountAccessReadWrite},
				},
			},
		},
	})
	actorCtx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ID:          "host-no-bash",
		NativeTools: models.NativeToolConfig{FileIO: true},
	})
	_, err := exec.Execute(actorCtx, "bash", map[string]any{"command": "printf should-not-run > denied.txt"})
	requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "tool_not_allowed")
	if _, statErr := os.Stat(filepath.Join(workspaceDir, "denied.txt")); !os.IsNotExist(statErr) {
		t.Fatalf("unauthorized host bash created file; stat err=%v", statErr)
	}
}

func containsToolDefinition(defs []llm.ToolDefinition, name string) bool {
	for _, def := range defs {
		if strings.TrimSpace(def.Name) == name {
			return true
		}
	}
	return false
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
