package canonicalrouting

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func CopyStateOnlyAcquisition(t testing.TB, workflowName string, modes map[string]string, targetFlow string) string {
	t.Helper()
	if workflowName == "" || targetFlow == "" || modes[targetFlow] == "" {
		t.Fatal("state-only acquisition fixture requires an exact target flow")
	}
	root := t.TempDir()
	rootSchema := "name: " + workflowName + "\npins:\n  inputs:\n    events: [test.node_emitted.selector, test.node_emitted.upserter]\n"
	if targetFlow != "." {
		rootSchema += "  outputs:\n    events: [test.node_emitted.selector, test.node_emitted.upserter]\nconnect:\n"
		for _, name := range []string{"test.node_emitted.selector", "test.node_emitted.upserter"} {
			rootSchema += fmt.Sprintf("  - {event: %s, from: ., to: %s}\n", name, targetFlow)
		}
	}
	writeClosedVariantFile(t, root, "schema.yaml", rootSchema)
	const eventSchemas = "test.node_emitted.selector:\n  account_id: text\n  instance_key: text\ntest.node_emitted.upserter:\n  account_id: text\n  instance_key: text\n"
	writeClosedVariantFile(t, root, "events.yaml", eventSchemas)
	paths := make([]string, 0, len(modes))
	for path, mode := range modes {
		switch mode {
		case runtimecontracts.FlowModeStatic, runtimecontracts.FlowModeSingleton, runtimecontracts.FlowModeTemplate:
		default:
			t.Fatalf("state-only acquisition fixture has unsupported mode %q", mode)
		}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		mode := modes[path]
		schema := fmt.Sprintf("name: %s\ninitial_state: active\nstates: [active, done]\nterminal_states: [done]\n", filepath.Base(path))
		if path == "." {
			schema = strings.Replace(schema, "name: .", "name: "+workflowName, 1) + strings.TrimPrefix(rootSchema, "name: "+workflowName+"\n")
		} else {
			schema += "mode: " + mode + "\n"
			if mode == runtimecontracts.FlowModeTemplate {
				schema += "instance: instance_key\n"
			}
			if path == targetFlow {
				if mode == runtimecontracts.FlowModeTemplate {
					schema += "pins:\n  inputs:\n    events:\n      - {event: test.node_emitted.selector, resolution: {mode: select}}\n      - {event: test.node_emitted.upserter, resolution: {mode: select-or-create}}\n"
				} else {
					schema += "pins:\n  inputs:\n    events: [test.node_emitted.selector, test.node_emitted.upserter]\n"
				}
			}
		}
		writeClosedVariantFile(t, root, filepath.ToSlash(filepath.Join(path, "schema.yaml")), schema)
		writeClosedVariantFile(t, root, filepath.ToSlash(filepath.Join(path, "entities.yaml")), "review_item:\n  account_id: text\n  instance_key: text\n  items: \"[json]\"\n")
		if path != "." && path != targetFlow {
			writeClosedVariantFile(t, root, filepath.ToSlash(filepath.Join(path, "events.yaml")), eventSchemas)
		}
		writeClosedVariantFile(t, root, filepath.ToSlash(filepath.Join(path, "nodes.yaml")), `selector:
  execution_type: system_node
  subscribes_to: [test.node_emitted.selector]
  event_handlers:
    test.node_emitted.selector:
      accumulate: {into: items, from: payload}
upserter:
  execution_type: system_node
  subscribes_to: [test.node_emitted.upserter]
  event_handlers:
    test.node_emitted.upserter:
      create_entity: true
`)
	}
	return root
}
