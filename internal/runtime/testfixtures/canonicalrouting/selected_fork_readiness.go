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
		for _, file := range []string{"worker-flow/events.yaml", "worker-flow/nodes.yaml"} {
			if err := os.Remove(filepath.Join(root, file)); err != nil {
				t.Fatal(err)
			}
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
		"events.yaml":            "inspect-events.yaml",
		"worker-flow/nodes.yaml": node,
	} {
		addition, err := os.ReadFile(filepath.Join(RepoRoot(t), "internal/runtime/cataloge2e/testdata", "terminal-retirement", fixture))
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(frontier, "failure") && path == "worker-flow/nodes.yaml" {
			addition = []byte(strings.ReplaceAll(string(addition), "terminal_probe.succeeded", "terminal_probe.failed"))
		}
		if frontier == "activity_loop_rule" && path == "worker-flow/nodes.yaml" {
			addition = []byte(strings.ReplaceAll(string(addition), "      advances_to: executing\n", "      advances_to: working\n      rules:\n        - else: true\n          advances_to: executing\n"))
			addition = []byte(strings.ReplaceAll(string(addition), "      activity:\n        id: terminal_probe\n        tool: terminal_probe\n        input: {}", "          activity:\n            id: terminal_probe\n            tool: terminal_probe\n            input: {}"))
			addition = []byte(strings.ReplaceAll(string(addition), "            input: {}\n", "            input: {}\n        - else: true\n          advances_to: executing\n          activity:\n            id: unselected_probe\n            tool: terminal_probe\n            input: {}\n"))
		}
		file := filepath.Join(root, path)
		data, err := os.ReadFile(file)
		if err != nil && !os.IsNotExist(err) {
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
				if err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				data = append(existing, data...)
			}
			if err := os.WriteFile(filepath.Join(root, path), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}

	rootSchema, err := os.ReadFile(filepath.Join(RepoRoot(t), "internal/runtime/cataloge2e/testdata", "terminal-retirement", "root-schema.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	childSchemaPath := filepath.Join(root, "worker-flow/schema.yaml")
	childSchema, err := os.ReadFile(childSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	childSchema = []byte(strings.Replace(string(childSchema), "      - event: worker.ready", "      - event: worker.inspect\n        resolution: {mode: select}\n      - event: worker.ready", 1))
	if declarations == 0 {
		childSchema = []byte(strings.Replace(string(childSchema), "  outputs:\n    events: [worker.observed]\n", "", 1))
		childSchema = []byte(strings.Replace(string(childSchema), "      - event: worker.ready\n        resolution: {mode: select}\n", "", 1))
		rootSchema = []byte(strings.ReplaceAll(string(rootSchema), "[worker.ready, ", "["))
		rootSchema = []byte(strings.Replace(string(rootSchema), "  - {event: worker.ready, from: ., to: worker-flow}\n", "", 1))
		rootEventsPath := filepath.Join(root, "events.yaml")
		rootEvents, err := os.ReadFile(rootEventsPath)
		if err != nil {
			t.Fatal(err)
		}
		rootEvents = []byte(strings.Replace(string(rootEvents), "worker.ready:\n  worker_id: text\n", "", 1))
		if err := os.WriteFile(rootEventsPath, rootEvents, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if frontier == "mixed" || frontier == "mixed_progress" {
		rootSchema = []byte(strings.ReplaceAll(string(rootSchema), ", worker.inspect", ""))
		rootSchema = []byte(strings.Replace(string(rootSchema), "  - {event: worker.inspect, from: ., to: worker-flow}\n", "", 1))
		childSchema = []byte(strings.Replace(string(childSchema), "      - event: worker.inspect\n        resolution: {mode: select}\n", "", 1))
		rootEvents, err := os.ReadFile(filepath.Join(root, "events.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		rootEvents = []byte(strings.Replace(string(rootEvents), "worker.inspect:\n  worker_id: text\n", "", 1))
		if err := os.WriteFile(filepath.Join(root, "events.yaml"), rootEvents, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if strings.HasPrefix(frontier, "activity_loop") {
		rootSchema = []byte(strings.ReplaceAll(string(rootSchema), "worker.inspect]", "worker.inspect, worker.retry]"))
		rootSchema = append(rootSchema, []byte("  - {event: worker.retry, from: ., to: worker-flow}\n")...)
		childSchema = []byte(strings.Replace(string(childSchema), "    events:\n", "    events:\n      - event: worker.retry\n        resolution: {mode: select}\n", 1))
		childEvents, err := os.ReadFile(filepath.Join(root, "worker-flow/events.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		const retry = "worker.retry:\n  worker_id: text\n  revision_id: {type: text}\n"
		childEvents = []byte(strings.Replace(string(childEvents), retry, "", 1))
		if err := os.WriteFile(filepath.Join(root, "worker-flow/events.yaml"), childEvents, 0600); err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(filepath.Join(root, "events.yaml"), os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString(retry)
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("write retry input schema: %v %v", err, closeErr)
		}
	}
	// Root inputs enter through explicit connections. The readiness frontier is
	// the resulting local publication, not an unsupported dynamic-connect fork.
	for _, event := range []string{"worker.ready", "worker.inspect"} {
		if !strings.Contains(string(rootSchema), "  - {event: "+event+",") {
			continue
		}
		input := event + ".requested"
		rootSchema = []byte(strings.Replace(string(rootSchema), "{event: "+event+", from: ., to: worker-flow}", "{event: "+event+", from: ., to: worker-flow, rename: "+input+"}", 1))
		childSchema = []byte(strings.Replace(string(childSchema), "- event: "+event+"\n", "- event: "+input+"\n", 1))
		for file, addition := range map[string]string{
			"worker-flow/events.yaml": event + ":\n  worker_id: text\n",
			"worker-flow/nodes.yaml":  fmt.Sprintf("\n%s-entry:\n  execution_type: system_node\n  subscribes_to: [%s]\n  event_handlers:\n    %s:\n      emit:\n        event: %s\n        fields:\n          worker_id: payload.worker_id\n", strings.ReplaceAll(event, ".", "-"), input, input, event),
		} {
			path := filepath.Join(root, file)
			data, err := os.ReadFile(path)
			if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, append(data, addition...), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), rootSchema, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(childSchemaPath, childSchema, 0600); err != nil {
		t.Fatal(err)
	}
	return root
}
