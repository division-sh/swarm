package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestNodeValueFieldsPreserveNestedSourceAndRejectAuthoredPresence(t *testing.T) {
	source, err := yamlsource.Load([]byte("node:\n  handler: &row\n    from: null\n  other: *row\n"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := nodeValueFields(source.Document("nodes.yaml").Root(), "nodes", map[string]struct{}{"node": {}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	node, err := nodeValueFields(root["node"], "node", map[string]struct{}{"handler": {}, "other": {}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = nodeValueFields(node["other"], "handler", map[string]struct{}{}, map[string]string{"from": "not executable"})
	if err == nil || !strings.Contains(err.Error(), "nodes.yaml:") || !strings.Contains(err.Error(), "from") {
		t.Fatalf("expected source-located authored-presence rejection, got %v", err)
	}
}

func TestNodeValueFieldsRejectDuplicateEffectiveAliasKeys(t *testing.T) {
	source, err := yamlsource.Load([]byte("node:\n  handler: &row {emit: one}\n  duplicate:\n    <<: *row\n    emit: two\n"))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := source.Document("nodes.yaml").Root().Lookup("node")
	if err != nil {
		t.Fatal(err)
	}
	_, err = nodeValueFields(nodes.Value, "node", map[string]struct{}{"handler": {}, "duplicate": {}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := nodes.Value.Lookup("duplicate")
	if err != nil {
		t.Fatal(err)
	}
	_, err = nodeValueFields(duplicate.Value, "handler", map[string]struct{}{"emit": {}}, nil)
	if err == nil || !strings.Contains(err.Error(), "duplicate effective YAML key") || !strings.Contains(err.Error(), "nodes.yaml:") {
		t.Fatalf("expected source-located duplicate effective key, got %v", err)
	}
}

func TestProjectNodeTimerValuePreservesNestedCoordinates(t *testing.T) {
	source, err := yamlsource.Load([]byte("node:\n  timers:\n    - {id: retry, delay: 1m, recurring: true}\n"))
	if err != nil {
		t.Fatal(err)
	}
	root := source.Document("nodes.yaml").Root()
	nodes, err := root.Lookup("node")
	if err != nil {
		t.Fatal(err)
	}
	timers, err := nodes.Value.Lookup("timers")
	if err != nil {
		t.Fatal(err)
	}
	items, err := timers.Value.Sequence()
	if err != nil {
		t.Fatal(err)
	}
	timer, err := projectNodeTimerValue(items[0])
	if err != nil {
		t.Fatal(err)
	}
	if timer.ID != "retry" || timer.Delay != "1m" || !timer.Recurring {
		t.Fatalf("wrong timer projection: %#v", timer)
	}
	retired, err := yamlsource.Load([]byte("node:\n  timers:\n    - {delay_seconds: 7}\n"))
	if err != nil {
		t.Fatal(err)
	}
	nodes, _ = retired.Document("nodes.yaml").Root().Lookup("node")
	timers, _ = nodes.Value.Lookup("timers")
	items, _ = timers.Value.Sequence()
	_, err = projectNodeTimerValue(items[0])
	if err == nil || !strings.Contains(err.Error(), "nodes.yaml:3:") || !strings.Contains(err.Error(), "delay_seconds") {
		t.Fatalf("expected source-located retired timer field, got %v", err)
	}
}

func TestProjectNodeEmitValuePreservesR2AndRetiredRouting(t *testing.T) {
	source, err := yamlsource.Load([]byte("emit: &reply\n  event: task.completed\n  fields: {answer: '${payload.answer}', fixed: null}\nother: *reply\n"))
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := source.Document("nodes.yaml").Root().Lookup("other")
	if err != nil {
		t.Fatal(err)
	}
	emit, err := projectNodeEmitValue(lookup.Value)
	if err != nil {
		t.Fatal(err)
	}
	if emit.Event != "task.completed" || !emit.Fields["answer"].HasCELValue() || !emit.Fields["fixed"].HasLiteralValue() {
		t.Fatalf("wrong emit projection: %#v", emit)
	}
	retired, err := yamlsource.Load([]byte("emit: {event: task.completed, broadcast: false}\n"))
	if err != nil {
		t.Fatal(err)
	}
	lookup, _ = retired.Document("nodes.yaml").Root().Lookup("emit")
	_, err = projectNodeEmitValue(lookup.Value)
	if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "nodes.yaml:") {
		t.Fatalf("expected source-located routing retirement, got %v", err)
	}
}
