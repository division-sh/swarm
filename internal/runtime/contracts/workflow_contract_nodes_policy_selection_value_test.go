package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodePolicySelectionValues(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte(`worker:
  event_handlers:
    task.ready:
      rules:
        - case: {selector: payload.region, equals: eu}
          emit: task.eu
        - range: {value: payload.score, gte: 10, lt: 20}
          emit: task.mid
        - else: true
          emit: task.other
`))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	rules := nodes["worker"].EventHandlers["task.ready"].Rules
	if len(rules) != 3 || rules[0].Condition != `payload.region == "eu"` ||
		rules[0].PolicyRow.Kind != PolicySheetRowKindCase ||
		rules[1].Condition != "payload.score >= 10 && payload.score < 20" ||
		rules[1].PolicyRow.Kind != PolicySheetRowKindRange {
		t.Fatalf("selection rows not projected: %#v", rules)
	}
}

func TestProjectNodePolicySelectionRejectsDynamicRangeBound(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("worker:\n  event_handlers:\n    task.ready:\n      rules:\n        - range: {value: payload.score, gt: payload.min}\n        - else: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), "dynamic bound") || !strings.Contains(err.Error(), "nodes.yaml:5:") {
		t.Fatalf("expected source-located dynamic-bound rejection, got %v", err)
	}
}
