package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestNodeShapeRewriteKeepsOrderStyleAndHandlerBodies(t *testing.T) {
	source := []byte(`zeta:
  execution_type: system_node
  subscribes_to: [work.requested, work.*]
  produces: [work.completed]
  timers:
    - id: reminder
      event: work.requested
      delay: 1s
  event_handlers:
    work.requested:
      guard:
        check: |-
          payload.ready &&
          payload.score > 0
      emit:
        event: work.completed
        fields:
          single: 'kept ${payload.ready}'
          double: "kept ${payload.ready}"
          data: {event_handlers: literal, subscribes_to: literal}
    work.*: {}
  gate_state: {ready: ready}
alpha:
  event_handlers:
    request: {}
`)
	output, counts, err := rewriteNodeSource(source)
	if err != nil || counts != (rewriteCounts{Subscriptions: 1, ExecutionTypes: 1, Produces: 1, HandlerContainers: 2}) {
		t.Fatalf("rewrite counts=%+v error=%v", counts, err)
	}
	var before, after yaml.Node
	if err := yaml.Unmarshal(source, &before); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(output, &after); err != nil {
		t.Fatal(err)
	}
	root := after.Content[0]
	if root.Content[0].Value != "zeta" || root.Content[2].Value != "alpha" {
		t.Fatal("node declarations were sorted")
	}
	node := root.Content[1]
	if got := []string{node.Content[0].Value, node.Content[2].Value, node.Content[4].Value, node.Content[6].Value}; strings.Join(got, ",") != "timers,work.requested,work.*,gate_state" {
		t.Fatalf("field/handler order changed: %v", got)
	}
	beforeHandlers := before.Content[0].Content[1].Content[9]
	for i := range beforeHandlers.Content {
		assertSameYAMLValue(t, beforeHandlers.Content[i], node.Content[i+2])
	}
	again, againCounts, err := rewriteNodeSource(output)
	if err != nil || againCounts.changed() || !bytes.Equal(again, output) {
		t.Fatalf("rewrite is not byte-idempotent: counts=%+v error=%v", againCounts, err)
	}
}

func assertSameYAMLValue(t *testing.T, before, after *yaml.Node) {
	t.Helper()
	if before.Kind != after.Kind || before.Tag != after.Tag || before.Value != after.Value || before.Style != after.Style || len(before.Content) != len(after.Content) {
		t.Fatalf("handler value/style changed: before=%#v after=%#v", before, after)
	}
	for i := range before.Content {
		assertSameYAMLValue(t, before.Content[i], after.Content[i])
	}
}

func TestNodeShapeRewriteKeepsNodeAliasesAndMergeOrder(t *testing.T) {
	source := []byte("first: &node\n  subscribes_to: [ready]\n  event_handlers:\n    ready: {}\nsecond: *node\nthird:\n  <<: *node\n  timers: []\n")
	output, counts, err := rewriteNodeSource(source)
	if err != nil || counts.Subscriptions != 1 || counts.HandlerContainers != 1 {
		t.Fatalf("rewrite counts=%+v error=%v", counts, err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(output, &document); err != nil {
		t.Fatal(err)
	}
	root := document.Content[0]
	if root.Content[1].Anchor != "node" || root.Content[3].Alias != root.Content[1] || root.Content[5].Content[1].Alias != root.Content[1] || root.Content[5].Content[0].Value != "<<" {
		t.Fatal("node alias/merge identity or ordering changed")
	}
	if root.Content[1].Content[0].Value != "ready" {
		t.Fatal("aliased node did not consume the rewritten handlers")
	}
}

func TestNodeShapeRewriteRequiresExplicitUnsafeSourceDisposition(t *testing.T) {
	for _, tc := range []struct{ source, want string }{
		{"worker:\n  event_handlers: &handlers {ready: {}}\n", "needs an explicit source-ledger rewrite"},
		{"first: &handlers {ready: {}}\nworker:\n  event_handlers: *handlers\n", "needs an explicit source-ledger rewrite"},
		{"worker:\n  event_handlers: null\n", "needs an explicit source-ledger rewrite"},
		{"worker:\n  event_handlers: {ready: {}}\n", "needs an explicit source-ledger rewrite"},
		{"worker: &node\n  <<: *node\n", "YAML-ALIAS-CYCLE"},
		{"worker:\n  subscribes_to: [first]\n  subscribes_to: [second]\n  event_handlers: {}\n", "duplicate effective YAML key"},
		{"worker:\n  event_handlers: {first: {}}\n  event_handlers: {second: {}}\n", "duplicate effective YAML key"},
		{"worker:\n  timers: []\n  event_handlers:\n    timers: {}\n", "duplicate effective YAML key"},
	} {
		output, counts, err := rewriteNodeSource([]byte(tc.source))
		if err == nil || !strings.Contains(err.Error(), tc.want) || output != nil || counts.changed() {
			t.Fatalf("unsafe source silently rewritten or wrong refusal: output=%q counts=%+v error=%v want=%q", output, counts, err, tc.want)
		}
	}
}

func TestNodeShapeRewritePreservesExplicitEmptyNode(t *testing.T) {
	for _, source := range []string{
		"worker:\n  event_handlers: {}\n",
		"worker:\n  execution_type: system_node\n",
		"worker:\n  produces: []\n  event_handlers: {}\n",
	} {
		output, counts, err := rewriteNodeSource([]byte(source))
		if err != nil || !counts.changed() {
			t.Fatalf("empty node rewrite counts=%+v error=%v", counts, err)
		}
		var after yaml.Node
		if err := yaml.Unmarshal(output, &after); err != nil {
			t.Fatal(err)
		}
		node := after.Content[0].Content[1]
		if node.Kind != yaml.MappingNode || len(node.Content) != 0 {
			t.Fatalf("empty node became null or gained invented fields: %q", output)
		}
	}
}

func TestNodeShapeRewriteDiskCorpusPreservesEveryHandler(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("git", "ls-files", "*nodes.yaml")
	command.Dir = root
	paths, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	files := strings.Fields(string(paths))
	if len(files) == 0 {
		t.Fatal("no checked-in node declaration selected")
	}
	var handlers int
	for _, path := range files {
		source, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		output, _, err := rewriteNodeSource(source)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		var before, after yaml.Node
		if err := yaml.Unmarshal(source, &before); err != nil {
			t.Fatal(err)
		}
		if err := yaml.Unmarshal(output, &after); err != nil {
			t.Fatal(err)
		}
		oldRoot, newRoot := before.Content[0], after.Content[0]
		if len(oldRoot.Content) != len(newRoot.Content) {
			t.Fatalf("%s: declaration membership changed", path)
		}
		for i := 0; i < len(oldRoot.Content); i += 2 {
			if oldRoot.Content[i].Value != newRoot.Content[i].Value {
				t.Fatalf("%s: declaration order changed", path)
			}
			oldNode, newNode := oldRoot.Content[i+1], newRoot.Content[i+1]
			for j := 0; j < len(oldNode.Content); j += 2 {
				key, value := oldNode.Content[j], oldNode.Content[j+1]
				switch key.Value {
				case "subscribes_to", "execution_type", "produces":
					continue
				case "event_handlers":
					for k := 0; k < len(value.Content); k += 2 {
						assertSameYAMLValue(t, value.Content[k+1], requireMappingValue(t, newNode, value.Content[k].Value))
						handlers++
					}
				default:
					assertSameYAMLValue(t, value, requireMappingValue(t, newNode, key.Value))
				}
			}
		}
		again, counts, err := rewriteNodeSource(output)
		if err != nil || counts.changed() || !bytes.Equal(again, output) {
			t.Fatalf("%s: rewrite is not byte-idempotent: counts=%+v error=%v", path, counts, err)
		}
	}
	t.Logf("preserved all handlers=%d across files=%d", handlers, len(files))
}

func requireMappingValue(t *testing.T, node *yaml.Node, name string) *yaml.Node {
	t.Helper()
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == name {
			return node.Content[i+1]
		}
	}
	t.Fatalf("rewritten node lost field/handler %q", name)
	return nil
}

func TestNodeShapeRewriteDryRunNeverChangesSelectedFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "nodes.yaml")
	source := []byte("worker:\n  event_handlers:\n    ready: {}\n")
	if err := os.WriteFile(path, source, 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{path}, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, source) {
		t.Fatalf("dry run changed source: %q error=%v", got, err)
	}
	if err := run([]string{filepath.Join(root, "state.yaml")}, true); err == nil {
		t.Fatal("data/non-node source selected implicitly")
	}
	invalidRoot := t.TempDir()
	invalidPath := filepath.Join(invalidRoot, "nodes.yaml")
	if err := os.WriteFile(invalidPath, []byte("worker: {event_handlers: null}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{path, invalidPath}, true); err == nil {
		t.Fatal("invalid selected source silently rewritten")
	}
	got, err = os.ReadFile(path)
	if err != nil || !bytes.Equal(got, source) {
		t.Fatalf("later invalid source left earlier file partially rewritten: %q error=%v", got, err)
	}
}
