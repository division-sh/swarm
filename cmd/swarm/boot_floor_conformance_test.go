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
		Verbose:              true,
		NoRequireBundleMatch: true,
		TestLLMRuntime:       bootFloorNativeFallbackRuntime{},
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
	output := serve.outputString()
	for _, want := range []string{
		"workspace backend: host",
		"native_tools.bash",
		"UNSAFE: grants the agent execution on this machine",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("serve output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(strings.ToLower(output), "docker is not available") {
		t.Fatalf("host opt-out serve output shows Docker dependency despite explicit host backend:\n%s", output)
	}
}

func TestBootFloorConformanceVerifyDescribeReportNativeBashWorkspaceRequirement(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	contractsRoot := writeServeRuntimeNativeBashFixture(t)

	t.Run("verify text", func(t *testing.T) {
		opts := defaultVerifyCommandOptions()
		opts.contractsPath = contractsRoot

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

func assertBootFloorWorkspaceRequirementOutput(t *testing.T, output string) {
	t.Helper()
	for _, want := range []string{"workspace backend: docker", "native_tools.bash"} {
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
