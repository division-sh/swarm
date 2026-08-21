package cliapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyCommandLoadsPackageRelativeMockModuleStandaloneAndImported(t *testing.T) {
	root := t.TempDir()
	child := filepath.Join(root, "bot")
	writeVerifyMockPackageFile(t, filepath.Join(root, "package.yaml"), `name: outer
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - id: bot
    path: bot
flows: []
`)
	writeVerifyMockPackageFile(t, filepath.Join(root, "events.yaml"), "assistant.requested:\n  message: text\n")
	writeVerifyMockPackageFile(t, filepath.Join(child, "package.yaml"), `name: bot
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeVerifyMockPackageFile(t, filepath.Join(child, "agents.yaml"), `assistant:
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
	writeVerifyMockPackageFile(t, filepath.Join(child, "events.yaml"), `assistant.requested:
  message: text
`)
	writeVerifyMockPackageFile(t, filepath.Join(child, "mocks", "assistant.py"), "def handle(input):\n    return {'text': 'verified'}\n")
	config := writeTestVerifyRuntimeConfig(t)
	for _, contractsRoot := range []string{child, root} {
		t.Run(filepath.Base(contractsRoot), func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := executeRootCommand(context.Background(), RepoRoot(), []string{
				"verify", "--contracts", contractsRoot, "--config", config,
			}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("verify %s code=%d stdout=%s stderr=%s", contractsRoot, code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stdout.String(), "verify ok: contracts="+contractsRoot) {
				t.Fatalf("verify %s output missing success marker: %s", contractsRoot, stdout.String())
			}
		})
	}
}

func writeVerifyMockPackageFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
