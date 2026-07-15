package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/mockperformance"
)

func TestMaterializeAgentMockPerformancesCapturesExactGenerationBytes(t *testing.T) {
	root := t.TempDir()
	module := filepath.Join(root, "mocks", "assistant.py")
	if err := os.MkdirAll(filepath.Dir(module), 0o755); err != nil {
		t.Fatal(err)
	}
	original := []byte("def handle(input):\n    return {'text': 'first'}\n")
	if err := os.WriteFile(module, original, 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := materializeAgentMockPerformances(root, filepath.Join(root, "agents.yaml"), map[string]AgentRegistryEntry{
		"assistant": {Mock: mockperformance.Performance{Kind: "python", Module: "mocks/assistant.py"}},
	})
	if err != nil {
		t.Fatalf("materialize mock performance: %v", err)
	}
	performance := entries["assistant"].Mock
	if string(performance.Source) != string(original) || !strings.HasPrefix(performance.Digest, "sha256:") || performance.SourcePath != "mocks/assistant.py" {
		t.Fatalf("materialized performance = %#v", performance)
	}
	if err := os.WriteFile(module, []byte("def handle(input):\n    return {'text': 'second'}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if string(performance.Source) != string(original) {
		t.Fatalf("compiled generation reread ambient module: %q", performance.Source)
	}
}

func TestMaterializeAgentMockPerformancesRejectsOutsideAndInvalidModules(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.py")
	if err := os.WriteFile(outside, []byte("def handle(input): return {'text': 'bad'}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		module string
		want   string
	}{
		"traversal": {module: "../outside.py", want: "below the contracts root"},
		"missing":   {module: "mocks/missing.py", want: "cannot be read"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := materializeAgentMockPerformances(root, "agents.yaml", map[string]AgentRegistryEntry{
				"assistant": {Mock: mockperformance.Performance{Kind: "python", Module: tc.module}},
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}
