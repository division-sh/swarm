package main

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestBootFloorConformanceNativeBashHostOptOutIsLoudUnsafe(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	missingDocker := filepath.Join(t.TempDir(), "missing-docker")
	hostRoot := filepath.Join(t.TempDir(), "host-workspaces")

	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath: writeStoreBackendRuntimeConfigWithWorkspaceFields(t, storebackend.BackendSQLite.String(), filepath.Join(t.TempDir(), "runtime.db"), []string{
			fmt.Sprintf("  docker_bin: %q", missingDocker),
			fmt.Sprintf("  host_root: %q", hostRoot),
		}),
		ContractsPath:        writeServeRuntimeNativeBashFixture(t),
		DataSource:           t.TempDir(),
		WorkspaceBackend:     workspace.BackendHost,
		WorkspaceBackendSet:  true,
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            storebackend.ActiveDefaultBackend().String(),
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		RequireBundleMatch:   false,
		ShutdownGrace:        runtimepkg.DefaultShutdownGrace,
		NoRequireBundleMatch: true,
		TestLLMRuntime:       bootFloorNativeFallbackRuntime{},
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
	output := serve.outputString()
	for _, want := range []string{
		"workspace                  host · agent \"native-bash-worker\" runs on this machine",
		"WARNING: host workspace lets agents execute on this machine",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("serve output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(strings.ToLower(output), "docker is not reachable") {
		t.Fatalf("host opt-out serve output shows Docker dependency despite explicit host backend:\n%s", output)
	}
	if !strings.Contains(output, "swarm serve · ") || strings.Contains(output, "[1/22]") || strings.Contains(output, "\x1b[") {
		t.Fatalf("default serve did not use concise non-TTY lifecycle presentation:\n%s", output)
	}
}

func TestBootFloorConformanceVerifyDescribeReportNativeBashWorkspaceRequirement(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	contractsRoot := writeServeRuntimeNativeBashFixture(t)

	t.Run("verify text", func(t *testing.T) {
		opts := defaultVerifyCommandOptions()
		opts.contractsPath = contractsRoot
		opts.configPath = writeTestVerifyRuntimeConfig(t)

		var stdout, stderr bytes.Buffer
		code := runVerifyCommandWithOutput(context.Background(), repoRoot(), opts, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("verify code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
		}
		assertBootFloorWorkspaceRequirementOutput(t, stdout.String())
	})

	t.Run("verify json", func(t *testing.T) {
		opts := defaultVerifyCommandOptions()
		opts.contractsPath = contractsRoot
		opts.configPath = writeTestVerifyRuntimeConfig(t)
		opts.output.asJSON = true

		var stdout, stderr bytes.Buffer
		code := runVerifyCommandWithOutput(context.Background(), repoRoot(), opts, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("verify --json code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
		}
		if strings.TrimSpace(stderr.String()) != "" {
			t.Fatalf("verify --json stderr = %q, want empty", stderr.String())
		}
		output := decodeOutputJSON[verifyCommandResult](t, stdout.String())
		assertBootFloorWorkspaceRequirementOutput(t, output.WorkspaceBackend)
	})

	t.Run("describe text", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
			"describe",
			"--contracts", contractsRoot,
		}, &stdout, &stderr, defaultRootCommandOptions())
		if code != 0 {
			t.Fatalf("describe code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
		}
		assertBootFloorWorkspaceRequirementOutput(t, stdout.String())
	})

	t.Run("describe json", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
			"describe",
			"--contracts", contractsRoot,
			"--json",
		}, &stdout, &stderr, defaultRootCommandOptions())
		if code != 0 {
			t.Fatalf("describe --json code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
		}
		if strings.TrimSpace(stderr.String()) != "" {
			t.Fatalf("describe --json stderr = %q, want empty", stderr.String())
		}
		output := decodeOutputJSON[describeCommandOutput](t, stdout.String())
		assertBootFloorWorkspaceRequirementOutput(t, output.WorkspaceBackend)
	})
}

func TestBootFloorExplicitHostRefusalAcrossServeVerifyDescribe(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	configPath := writeDoctorClaudeHostConfig(t, "")
	contractsPath := doctorAgentContractsPath

	t.Run("serve", func(t *testing.T) {
		var out lockedBuffer
		code := runServeRuntime(context.Background(), repoRoot(), serveOptions{
			ConfigPath:           configPath,
			ContractsPath:        contractsPath,
			DataSource:           t.TempDir(),
			PlatformSpecPath:     defaultPlatformSpecPath,
			StoreMode:            storebackend.ActiveDefaultBackend().String(),
			APIListenAddr:        "127.0.0.1:0",
			MCPListenAddr:        "127.0.0.1:0",
			SelfCheck:            true,
			RequireBundleMatch:   false,
			NoRequireBundleMatch: true,
			Output:               &out,
		})
		if code != cliExitRuntime {
			t.Fatalf("serve code = %d, want %d\n%s", code, cliExitRuntime, out.String())
		}
		assertClaudeHostRefusal(t, out.String())
	})

	t.Run("verify", func(t *testing.T) {
		opts := defaultVerifyCommandOptions()
		opts.configPath = configPath
		opts.contractsPath = contractsPath
		var stdout, stderr bytes.Buffer
		if code := runVerifyCommandWithOutput(context.Background(), repoRoot(), opts, &stdout, &stderr); code == 0 {
			t.Fatalf("verify unexpectedly succeeded stdout=%s stderr=%s", stdout.String(), stderr.String())
		}
		assertClaudeHostRefusal(t, stderr.String())
	})

	t.Run("describe", func(t *testing.T) {
		opts := defaultDescribeCommandOptions()
		opts.configPath = configPath
		opts.contractsPath = contractsPath
		var stdout, stderr bytes.Buffer
		if code := runDescribeCommandWithOutput(context.Background(), repoRoot(), opts, &stdout, &stderr); code == 0 {
			t.Fatalf("describe unexpectedly succeeded stdout=%s stderr=%s", stdout.String(), stderr.String())
		}
		assertClaudeHostRefusal(t, stderr.String())
	})
}

func assertClaudeHostRefusal(t *testing.T, output string) {
	t.Helper()
	for _, want := range []string{"uses claude_cli backend", "Use Docker", "llm.backend: anthropic", "Docker-free local run"} {
		if !strings.Contains(output, want) {
			t.Fatalf("explicit-host refusal missing %q:\n%s", want, output)
		}
	}
}

func assertBootFloorWorkspaceRequirementOutput(t *testing.T, output string) {
	t.Helper()
	for _, want := range []string{"workspace backend: docker", "agent native-bash-worker", "native_tools.bash"} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
}

type bootFloorNativeFallbackRuntime struct {
	runtimellm.NoopRuntime
}

func (bootFloorNativeFallbackRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.OpenAIResponsesProviderContract()
}
