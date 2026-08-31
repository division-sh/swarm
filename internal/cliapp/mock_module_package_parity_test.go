package cliapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyCommandLoadsFlowRelativeMockModuleStandaloneAndNested(t *testing.T) {
	root := t.TempDir()
	child := filepath.Join(root, "bot")

	writeVerifyMockFlowFile(t, filepath.Join(child, "agents.yaml"), `assistant:
  id: assistant
  role: helper
  model: regular
  intent: {inline: "Reply deterministically."}
  subscriptions: [assistant.requested]
  emit_events: [assistant.requested]
  mock:
    kind: python
    module: mocks/assistant.py
`)
	writeVerifyMockFlowFile(t, filepath.Join(child, "events.yaml"), `assistant.requested:
  message: text?
`)
	writeVerifyMockFlowFile(t, filepath.Join(child, "mocks", "assistant.py"), "def handle(input):\n    return {'text': 'verified'}\n")
	config := writeTestVerifyRuntimeConfig(t)
	for _, sourceRoot := range []string{child, root} {
		t.Run(filepath.Base(sourceRoot), func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := executeRootCommand(context.Background(), RepoRoot(), []string{
				"verify", sourceRoot, "--config", config,
			}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("verify %s code=%d stdout=%s stderr=%s", sourceRoot, code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stdout.String(), "verify ok: source="+sourceRoot) {
				t.Fatalf("verify %s output missing success marker: %s", sourceRoot, stdout.String())
			}
		})
	}
}

func writeVerifyMockFlowFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
