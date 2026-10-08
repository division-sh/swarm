package canonicalrouting

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func CopyStateOnlyAcquisition(t testing.TB, workflowName string, modes map[string]string, targetFlow string) string {
	t.Helper()
	if workflowName == "" || targetFlow == "" || modes[targetFlow] == "" {
		t.Fatal("state-only acquisition fixture requires an exact target flow")
	}
	root := t.TempDir()
	var keyedParents []string
	for path, mode := range modes {
		if mode == "template" && strings.HasPrefix(targetFlow, path+"/") {
			keyedParents = append(keyedParents, path)
		}
	}
	sort.Strings(keyedParents)
	rootSchema := "name: " + workflowName + "\npins:\n  inputs: [test.node_emitted.selector, test.node_emitted.upserter]\n"
	if len(keyedParents) != 0 {
		rootSchema = strings.Replace(rootSchema, "inputs: [test.node_emitted.selector, test.node_emitted.upserter]", "inputs: [test.node_emitted.selector, test.node_emitted.upserter, test.fixture_parent.construct]", 1)
	}
	if targetFlow != "." {
		rootSchema += "  outputs: [test.node_emitted.selector, test.node_emitted.upserter]\nconnect:\n"
		for _, name := range []string{"test.node_emitted.selector", "test.node_emitted.upserter"} {
			policy := ""
			if modes[targetFlow] == "template" {
				resolution := "select"
				if name == "test.node_emitted.upserter" {
					resolution = "select-or-create"
				}
				policy = ", resolution: " + resolution
			}
			rootSchema += fmt.Sprintf("  - {event: %s, from: ., to: %s%s}\n", name, targetFlow, policy)
		}
		for _, parent := range keyedParents {
			rootSchema += fmt.Sprintf("  - {event: test.fixture_parent.construct, from: ., to: %s, resolution: select-or-create}\n", parent)
			for _, name := range []string{"test.node_emitted.selector", "test.node_emitted.upserter"} {
				rootSchema += fmt.Sprintf("  - {event: %s, from: ., to: %s, resolution: select, key_from: payload.parent_key}\n", name, parent)
			}
		}
	}
	writeClosedVariantFile(t, root, "schema.yaml", rootSchema)
	const baseEventSchemas = "test.node_emitted.selector:\n  account_id: text\n  instance_key: text\ntest.node_emitted.upserter:\n  account_id: text\n  instance_key: text\n"
	eventSchemas := baseEventSchemas
	if len(keyedParents) != 0 {
		eventSchemas = strings.ReplaceAll(eventSchemas, "  instance_key: text\n", "  instance_key: text\n  parent_key: text\n")
		eventSchemas += "test.fixture_parent.construct:\n  account_id: text\n  instance_key: text\n"
	}
	writeClosedVariantFile(t, root, "events.yaml", eventSchemas)
	paths := make([]string, 0, len(modes))
	for path, mode := range modes {
		switch mode {
		case "static", "singleton", "template":
		default:
			t.Fatalf("state-only acquisition fixture has unsupported mode %q", mode)
		}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		mode := modes[path]
		parentConstructor := mode == "template" && strings.HasPrefix(targetFlow, path+"/")
		schema := fmt.Sprintf("name: %s\nstages:\n  active: {}\n  done: {final: true}\n", filepath.Base(path))
		if path == "." {
			schema = strings.Replace(schema, "name: .", "name: "+workflowName, 1) + strings.TrimPrefix(rootSchema, "name: "+workflowName+"\n")
		} else {
			if mode == "template" {
				schema += "instance: instance_key\n"
			}
			if path == targetFlow {
				schema += "pins:\n  inputs:\n    - test.node_emitted.selector\n    - test.node_emitted.upserter\n"
			} else if parentConstructor {
				schema += "pins:\n  inputs: [test.fixture_parent.construct, test.node_emitted.selector, test.node_emitted.upserter]\n"
			}
		}
		writeClosedVariantFile(t, root, filepath.ToSlash(filepath.Join(path, "schema.yaml")), schema)
		entity := "review_item:\n  account_id: {type: text, initial: different-business-key}\n  instance_key: {type: text, initial: instance}\n  items: {type: '[json]', initial: []}\n"
		if mode == "template" {
			entity = "review_item:\n  account_id: text\n  instance_key: text\n  items: {type: '[json]', initial: []}\n"
		}
		writeClosedVariantFile(t, root, filepath.ToSlash(filepath.Join(path, "entities.yaml")), entity)
		if path != "." && path != targetFlow {
			if !parentConstructor {
				writeClosedVariantFile(t, root, filepath.ToSlash(filepath.Join(path, "events.yaml")), baseEventSchemas)
			}
		}
		nodes := `selector:
  execution_type: system_node
  subscribes_to: [test.node_emitted.selector]
  event_handlers:
    test.node_emitted.selector:
      accumulate: {into: items, from: payload}
upserter:
  execution_type: system_node
  subscribes_to: [test.node_emitted.upserter]
  event_handlers:
    test.node_emitted.upserter: {}
`
		if parentConstructor {
			nodes += "parent_constructor:\n  execution_type: system_node\n  event_handlers:\n    test.fixture_parent.construct: {}\n"
		}
		writeClosedVariantFile(t, root, filepath.ToSlash(filepath.Join(path, "nodes.yaml")), nodes)
	}
	return root
}
