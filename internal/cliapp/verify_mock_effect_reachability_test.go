package cliapp

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyConsumesCanonicalMockEffectReachability(t *testing.T) {
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")
	unsetSecretEnvForTest(t, "provider_credential")
	unsetSecretEnvForTest(t, "PROVIDER_CREDENTIAL")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	configPath := writeTestVerifyRuntimeConfig(t)

	for _, tc := range []struct {
		name            string
		includeLive     bool
		includeActivity bool
		wantFailure     []string
	}{
		{name: "global live with all exact mocks needs no outbound credential"},
		{
			name:        "mixed source retains exact live actor and effect requirement",
			includeLive: true,
			wantFailure: []string{"live-agent", "provider_credential", "tool provider.send"},
		},
		{
			name:            "all mocks retain effect reachable from live workflow activity",
			includeActivity: true,
			wantFailure:     []string{"provider_credential", "tool provider.send", "stub-agent-node", "task.requested"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			contractsPath := writeVerifyMockConnectorFixture(t, tc.includeLive, tc.includeActivity)
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
				"verify", "--contracts", contractsPath, "--config", configPath,
			}, &stdout, &stderr, defaultRootCommandOptions())
			combined := stdout.String() + stderr.String()
			if len(tc.wantFailure) == 0 {
				if code != 0 {
					t.Fatalf("verify exit=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
				}
				if strings.Contains(combined, "provider_credential") {
					t.Fatalf("verify reported unreachable credential: %s", combined)
				}
				return
			}
			if code == 0 {
				t.Fatalf("verify unexpectedly succeeded: %s", combined)
			}
			for _, want := range tc.wantFailure {
				if !strings.Contains(combined, want) {
					t.Fatalf("verify output missing %q: %s", want, combined)
				}
			}
		})
	}
}

func writeVerifyMockConnectorFixture(t *testing.T, includeLive, includeActivity bool) string {
	t.Helper()
	root := writeDoctorMockExecutionFixture(t, doctorMockExecutionFixtureOptions{IncludeUnmocked: includeLive})
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `
provider.send:
  category: provider_connector
  description: send a provider message
  handler_type: http
  effect_class: non_idempotent_write
  credentials:
    - provider_credential
  input_schema:
    type: object
  output_schema:
    type: object
  response_success:
    kind: http_status_2xx
  http:
    method: POST
    url: https://provider.example/messages
`)
	if includeActivity {
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
stub-agent-node:
  id: stub-agent-node
  execution_type: system_node
  subscribes_to: [task.requested]
  produces: [task.completed]
  event_handlers:
    task.requested:
      advances_to: done
      emit:
        event: task.completed
      activity:
        id: provider_send
        tool: provider.send
        input: {}
`)
	}
	if _, ok := loadWorkflowValidationBundleAt(t, root).Tools["provider.send"]; !ok {
		t.Fatal("verification fixture omitted provider.send from the effective tool catalog")
	}
	return root
}
