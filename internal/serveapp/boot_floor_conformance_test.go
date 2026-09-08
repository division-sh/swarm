package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestBootFloorConformanceNativeBashHostOptOutIsLoudUnsafe(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	missingDocker := filepath.Join(t.TempDir(), "missing-docker")
	hostRoot := filepath.Join(t.TempDir(), "host-workspaces")

	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath: writeStoreBackendRuntimeConfigWithWorkspaceFields(t, storebackend.BackendSQLite.String(), filepath.Join(t.TempDir(), "runtime.db"), []string{
			fmt.Sprintf("  docker_bin: %q", missingDocker),
			fmt.Sprintf("  host_root: %q", hostRoot),
		}),
		SourceRoot:          writeServeRuntimeNativeBashFixture(t),
		WorkspaceBackend:    workspace.BackendHost,
		WorkspaceBackendSet: true,
		PlatformSpecPath:    defaultPlatformSpecPath,
		StoreMode:           storebackend.ActiveDefaultBackend().String(),
		APIListenAddr:       "127.0.0.1:0",
		MCPListenAddr:       "127.0.0.1:0",
		SelfCheck:           true,
		ShutdownGrace:       runtimepkg.DefaultShutdownGrace,
		TestLLMRuntime:      bootFloorNativeFallbackRuntime{},
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
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

func TestBootFloorStructuralReadersDoNotClaimNativeWorkspaceReadiness(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("PATH", t.TempDir())
	sourceRoot := writeServeRuntimeNativeBashFixture(t)
	configPath := writeTestVerifyRuntimeConfig(t)
	for _, command := range []string{"verify", "describe"} {
		for _, asJSON := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%t", command, asJSON), func(t *testing.T) {
				args := []string{command, sourceRoot, "--config", configPath}
				if asJSON {
					args = append(args, "--json")
				}
				var stdout, stderr bytes.Buffer
				if code := executeCLIFrom(context.Background(), repoRootForTest(), args, &stdout, &stderr, Run); code != 0 {
					t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
				}
				if asJSON {
					var output map[string]any
					if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
						t.Fatal(err)
					}
					if output["validation_scope"] != "structural" || output["live_readiness"] != "not_evaluated" {
						t.Fatalf("structural classification missing: %v", output)
					}
					for _, retired := range []string{"workspace_backend", "capability_subjects"} {
						if _, exists := output[retired]; exists {
							t.Fatalf("structural output still claims %s", retired)
						}
					}
				} else if !strings.Contains(stdout.String(), "validation: structural; live readiness: not evaluated") {
					t.Fatalf("structural classification missing: %s", stdout.String())
				}
				if strings.Contains(stdout.String(), "workspace backend:") {
					t.Fatal("structural read performed deployment workspace selection")
				}
			})
		}
	}
}

func TestBootFloorExplicitHostRefusalIsLiveAdmissionNotStructuralValidity(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	configPath := writeDoctorClaudeHostConfig(t, "")
	sourceRoot := writeServeRuntimeNativeBashFixture(t)
	t.Run("serve", func(t *testing.T) {
		var out lockedBuffer
		code := runFrom(context.Background(), repoRootForTest(), cliapp.ServeOptions{
			ConfigPath: configPath, SourceRoot: sourceRoot, PlatformSpecPath: defaultPlatformSpecPath,
			StoreMode: storebackend.ActiveDefaultBackend().String(), SwarmDir: t.TempDir(), SwarmDirSet: true,
			APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Output: &out,
		})
		if code == 0 {
			t.Fatalf("live host admission unexpectedly succeeded: %s", out.String())
		}
		assertClaudeHostRefusal(t, out.String())
	})
	for _, command := range []string{"verify", "describe"} {
		t.Run(command, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := executeCLIFrom(context.Background(), repoRootForTest(), []string{command, sourceRoot, "--config", configPath}, &stdout, &stderr, Run); code != 0 {
				t.Fatalf("structural read confused deployment readiness with validity: code=%d stderr=%s", code, stderr.String())
			}
		})
	}
}

func assertClaudeHostRefusal(t *testing.T, output string) {
	t.Helper()
	for _, want := range []string{"uses claude_cli backend", "Use Docker", "llm.backend: anthropic", "Docker-free local run"} {
		if !strings.Contains(output, want) {
			t.Fatalf("explicit-host refusal missing %q:\n%s", want, output)
		}
	}
}

type bootFloorNativeFallbackRuntime struct {
	runtimellm.NoopRuntime
}

func (bootFloorNativeFallbackRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.AnthropicAPIProviderContract()
}
