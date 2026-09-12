package canonicalrouting

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func CopySelectedForkReadiness(t testing.TB, declarations int, frontier string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS(filepath.Join(RepoRoot(t), "tests/tier12-runtime-fork/test-run-scoped-flow-agent-fork"))); err != nil {
		t.Fatal(err)
	}
	agentsPath := filepath.Join(root, "worker-flow/agents.yaml")
	agents, err := os.ReadFile(agentsPath)
	if err != nil {
		t.Fatal(err)
	}
	if declarations == 0 {
		if err := os.Remove(agentsPath); err != nil {
			t.Fatal(err)
		}
		eventsPath := filepath.Join(root, "worker-flow/events.yaml")
		data, err := os.ReadFile(filepath.Join(RepoRoot(t), "internal/runtime/cataloge2e/testdata", "terminal-retirement", "agent-free-events.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(frontier, "activity_loop") {
			data = append(data, []byte("  revision_id: {type: text}\n")...)
		}
		if err := os.WriteFile(eventsPath, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if declarations >= 2 {
		prompt, err := os.ReadFile(filepath.Join(root, "worker-flow/prompts/worker-agent.md"))
		if err != nil {
			t.Fatal(err)
		}
		fixturePath := filepath.Join(root, "tests/fixtures.yaml")
		fixture, err := os.ReadFile(fixturePath)
		if err != nil {
			t.Fatal(err)
		}
		allAgents := append([]byte(nil), agents...)
		allFixtures := append([]byte(nil), fixture...)
		for i := 1; i < declarations; i++ {
			name := fmt.Sprintf("additional-agent-%d", i)
			if i == 1 {
				name = "second-agent"
			}
			allAgents = append(allAgents, []byte(strings.ReplaceAll(string(agents), "worker-agent", name))...)
			allFixtures = append(allFixtures, []byte(strings.ReplaceAll(strings.TrimPrefix(string(fixture), "agent_fixtures:\n"), "worker-agent", name))...)
			if err := os.WriteFile(filepath.Join(root, "worker-flow/prompts/"+name+".md"), prompt, 0600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(agentsPath, allAgents, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fixturePath, allFixtures, 0600); err != nil {
			t.Fatal(err)
		}
	}
	node := "inspect-node.yaml"
	switch frontier {
	case "mixed":
		node = "mixed-node.yaml"
	case "mixed_progress":
		node = "mixed-progress-node.yaml"
	case "activity", "activity_failure", "activity_rejected", "activity_write", "activity_write_failure":
		node = "inspect-activity-node.yaml"
	case "activity_loop", "activity_loop_rule", "activity_loop_failure":
		node = "inspect-loop-activity-node.yaml"
	}
	for path, fixture := range map[string]string{
		"worker-flow/events.yaml": "inspect-events.yaml",
		"worker-flow/nodes.yaml":  node,
	} {
		addition, err := os.ReadFile(filepath.Join(RepoRoot(t), "internal/runtime/cataloge2e/testdata", "terminal-retirement", fixture))
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(frontier, "failure") && path == "worker-flow/nodes.yaml" {
			addition = []byte(strings.ReplaceAll(string(addition), "terminal_probe.succeeded", "terminal_probe.failed"))
		}
		if frontier == "activity_loop_rule" && path == "worker-flow/nodes.yaml" {
			addition = []byte(strings.ReplaceAll(string(addition), "      advances_to: executing\n", "      advances_to: working\n      rules:\n        - condition: else\n          advances_to: executing\n"))
			addition = []byte(strings.ReplaceAll(string(addition), "      activity:\n        id: terminal_probe\n        tool: terminal_probe\n        input: {}", "          activity:\n            id: terminal_probe\n            tool: terminal_probe\n            input: {}"))
			addition = []byte(strings.ReplaceAll(string(addition), "            input: {}\n", "            input: {}\n        - condition: else\n          advances_to: executing\n          activity:\n            id: unselected_probe\n            tool: terminal_probe\n            input: {}\n"))
		}
		file := filepath.Join(root, path)
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(frontier, "activity_loop") && path == "worker-flow/nodes.yaml" {
			data = []byte(strings.ReplaceAll(string(data), "      advances_to: complete", "      loop: {close: inspection, from: executing}\n      advances_to: complete"))
		}
		if err := os.WriteFile(file, append(data, addition...), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if strings.HasPrefix(frontier, "activity_loop") {
		for path, fixture := range map[string]string{"worker-flow/schema.yaml": "inspect-loop-schema.yaml", "worker-flow/events.yaml": "inspect-loop-events.yaml"} {
			data, err := os.ReadFile(filepath.Join(RepoRoot(t), "internal/runtime/cataloge2e/testdata", "terminal-retirement", fixture))
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(path, "events.yaml") {
				existing, err := os.ReadFile(filepath.Join(root, path))
				if err != nil {
					t.Fatal(err)
				}
				data = append(existing, data...)
			}
			if err := os.WriteFile(filepath.Join(root, path), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	return root
}
