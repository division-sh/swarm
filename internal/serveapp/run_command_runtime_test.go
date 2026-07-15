package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
)

func TestRunCommandLocalForegroundRendersRealExplicitHostRefusal(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	configPath := writeDoctorClaudeHostConfig(t, "")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"entity_id": "entity-1"})
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
		"--api-port", freeDoctorTCPPort(t),
	}, &stdout, &stderr, Run)
	if code != cliapp.CLIExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliapp.CLIExitRuntime, stdout.String(), stderr.String())
	}
	assertClaudeHostRefusal(t, stderr.String())
}

func writeRunCommandPayloadFile(t *testing.T, payload map[string]any) string {
	t.Helper()
	path := t.TempDir() + "/payload.json"
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	return path
}
