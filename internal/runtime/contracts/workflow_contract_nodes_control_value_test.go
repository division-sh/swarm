package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeControlValues(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte(`worker:
  task.requested:
    activity:
      id: provision
      tool: infra.provision
      input: {component_id: payload.component_id}
      approval: {decision: provision}
    guard: payload.component_id != ""
`))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	handler := nodes["worker"].EventHandlers["task.requested"]
	if handler.Activity.ID != "provision" || handler.Activity.Tool != "infra.provision" ||
		handler.Activity.Approval.Decision != "provision" || !handler.Activity.Input["component_id"].HasCELValue() {
		t.Fatalf("activity projection lost typed fields: %#v", handler.Activity)
	}
	if handler.Guard == nil || handler.Guard.Check != `payload.component_id != ""` {
		t.Fatalf("guard projection lost its scalar expression: %#v", handler.Guard)
	}
	for _, retired := range []string{`{check: true}`, `{checks: [{check: true}]}`, `{id: check}`, `{on_fail: reject}`, `"true"`, `null`} {
		var guard GuardSpec
		if err := decodeNodeTestYAML([]byte(retired), &guard); err == nil {
			t.Fatalf("retired guard shape accepted: %s", retired)
		}
	}
}

func TestProjectNodeControlValuesRejectNestedDuplicate(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("worker:\n  task.requested:\n    activity:\n      id: provision\n      approval: {decision: yes, decision: no}\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), "duplicate") || !strings.Contains(err.Error(), "nodes.yaml:5:") {
		t.Fatalf("expected nested duplicate source location, got %v", err)
	}
}
