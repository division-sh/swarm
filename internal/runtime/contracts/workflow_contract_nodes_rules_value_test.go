package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeRuleRowsValuePreservesOrderAndContext(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte(`worker:
  event_handlers:
    task.ready:
      rules:
        ready: {when: payload.ready, emit: task.accepted}
        fallback: {else: true, advances_to: stopped}
      on_complete:
        - {id: complete, condition: payload.done, emit: task.done}
`))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	handler := nodes["worker"].EventHandlers["task.ready"]
	if len(handler.Rules) != 2 || handler.Rules[0].ID != "ready" ||
		handler.Rules[0].PolicyRow.Kind != PolicySheetRowKindWhen ||
		handler.Rules[1].PolicyRow.Kind != PolicySheetRowKindDefault ||
		len(handler.OnComplete) != 1 || handler.OnComplete[0].Condition != "payload.done" {
		t.Fatalf("rule order or context lost: %#v", handler)
	}
}

func TestProjectNodeRuleRowsValueRejectsRetiredCondition(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("worker:\n  event_handlers:\n    task.ready:\n      rules:\n        - condition: payload.ready\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), "condition") || !strings.Contains(err.Error(), "nodes.yaml:5:") {
		t.Fatalf("expected source-located retired rule predicate, got %v", err)
	}
}
