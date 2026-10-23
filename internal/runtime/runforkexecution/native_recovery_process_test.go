package runforkexecution

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

type nativeSelectedCrashLoader struct {
	admittedFixtureSelectedContractSourceLoader
}

func (l nativeSelectedCrashLoader) LoadRunForkSelectedContractSourceForRequest(ctx context.Context, req SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error) {
	loaded, err := l.admittedFixtureSelectedContractSourceLoader.LoadRunForkSelectedContractSourceForRequest(ctx, req)
	if err != nil {
		return loaded, err
	}
	artifact, err := sourceartifact.AdmitDirectory(l.SourceRoot)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	projection, err := sourceartifact.MaterializeRuntimeProjection(artifact)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	loaded.RuntimeProjection, loaded.Cleanup = projection, projection.Release
	return loaded, nil
}

func nativeSelectedForkCrashFixture(t *testing.T, repo string) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "contracts")
	if err := os.CopyFS(root, os.DirFS(filepath.Join(repo, "internal/runtime/runforkexecution/testdata/selected_fork_flow_scoped_mcp"))); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "worker", "mocks"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, contents := range map[string]string{
		"agents.yaml": "test-agent:\n  role: test-agent\n  model: regular\n  intent: prompts/test-agent.md\n  subscriptions: [task.assigned]\n  emit_events: [task.completed]\n  mock:\n    kind: python\n    module: mocks/worker.py\n",
		"mocks/worker.py": `def handle(input):
    results = input["tool_results"] or []
    if not results:
        return {"calls": [{"name": "emit_task_completed", "arguments": {"fork_result": "selected-native-once"}}], "usage": {"input_tokens": 1, "output_tokens": 1}}
    return {"text": "selected-native-once committed", "usage": {"input_tokens": 1, "output_tokens": 1}}
`, "manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
	} {
		if err := os.WriteFile(filepath.Join(root, "worker", name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
