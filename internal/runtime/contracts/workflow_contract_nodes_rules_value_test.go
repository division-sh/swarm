package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestNodeRuleFieldOptionsPreserveExactContextVocabulary(t *testing.T) {
	for _, context := range []handlerRuleDecodeContext{"", handlerRuleDecodeContextRules, handlerRuleDecodeContextOnComplete, "unrecognized"} {
		t.Run(string(context), func(t *testing.T) {
			got := nodeRuleFieldOptions(context)
			wantCount := len(ruleFieldOptions)
			if context != handlerRuleDecodeContextOnComplete {
				wantCount--
			}
			if len(got) != wantCount {
				t.Fatalf("context %q fields=%d, want %d", context, len(got), wantCount)
			}
			for key := range ruleFieldOptions {
				_, present := got[key]
				if want := key != "condition" || context == handlerRuleDecodeContextOnComplete; present != want {
					t.Fatalf("context %q field %q presence=%t, want %t", context, key, present, want)
				}
			}
		})
	}
}

func TestProjectNodeRuleRowsValuePreservesOrderAndContext(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte(`worker:
  event_handlers:
    task.ready:
      rules:
        - {when: payload.ready, emit: task.accepted}
        - {advances_to: stopped}
      on_complete:
        - {condition: payload.done, emit: task.done}
`))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	handler := nodes["worker"].EventHandlers["task.ready"]
	if len(handler.Rules) != 2 || handler.Rules[0].DisplayLabel("rules", 0) != "rules[0]: when payload.ready" ||
		handler.Rules[0].Emit.EventType() != "task.accepted" || handler.Rules[1].AdvancesTo != "stopped" ||
		handler.Rules[0].PolicyRow.Kind != PolicySheetRowKindWhen ||
		handler.Rules[1].PolicyRow.Kind != PolicySheetRowKindDefault ||
		len(handler.OnComplete) != 1 || handler.OnComplete[0].Condition != "payload.done" ||
		handler.OnComplete[0].DisplayLabel("on_complete", 0) != "on_complete[0]: condition payload.done" ||
		handler.OnComplete[0].Emit.EventType() != "task.done" {
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
