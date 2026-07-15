package serveapp

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
)

func TestRunCommandLocalForegroundRendersRealWorkspaceStartupDiagnostics(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*testing.T) (string, string)
		want      []string
	}{
		{
			name: "Docker daemon unavailable",
			configure: func(t *testing.T) (string, string) {
				dockerBin := configureDoctorDockerStub(t)
				t.Setenv("SWARM_TEST_DOCKER_UNAVAILABLE", "1")
				return writeDoctorClaudeConfig(t, dockerBin), dockerBin + " info"
			},
			want: []string{"workspace_prerequisite/docker_unavailable", "Start the Docker daemon, then verify with", " info`"},
		},
		{
			name: "workspace image unavailable",
			configure: func(t *testing.T) (string, string) {
				dockerBin := configureDoctorDockerStub(t)
				t.Setenv("SWARM_TEST_DOCKER_IMAGE_MISSING", "1")
				return writeDoctorClaudeConfig(t, dockerBin), ""
			},
			want: []string{"workspace_prerequisite/workspace_image_unavailable", "swarm workspace build --backend claude_cli"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			setDoctorProviderSecret(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
			t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
			t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
			t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
			configPath, exactRecovery := tt.configure(t)
			payloadPath := writeRunCommandPayloadFile(t, map[string]any{"entity_id": "entity-1"})
			apiPort := freeDoctorTCPPort(t)
			var stdout, stderr bytes.Buffer
			code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
				"--swarm-dir", t.TempDir(),
				"run", "start",
				"--event", "task.requested",
				"--payload", payloadPath,
				"--config", configPath,
				"--backend", "claude_cli",
				"--contracts", doctorAgentContractsPath,
				"--data", t.TempDir(),
				"--platform-spec", defaultPlatformSpecPath,
				"--api-port", apiPort,
			}, &stdout, &stderr, Run)
			if code != cliapp.CLIExitRuntime {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliapp.CLIExitRuntime, stdout.String(), stderr.String())
			}
			for _, want := range tt.want {
				if !strings.Contains(stderr.String(), want) {
					t.Fatalf("local run stderr missing %q:\n%s", want, stderr.String())
				}
			}
			if exactRecovery != "" && !strings.Contains(stderr.String(), exactRecovery) {
				t.Fatalf("local run stderr missing exact configured recovery %q:\n%s", exactRecovery, stderr.String())
			}
			if !strings.Contains(stderr.String(), "local serve exited before readiness") {
				t.Fatalf("local run stderr missing startup failure boundary:\n%s", stderr.String())
			}
		})
	}
}
