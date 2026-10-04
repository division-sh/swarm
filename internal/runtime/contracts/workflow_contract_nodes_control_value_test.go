package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeControlValues(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte(`worker:
  event_handlers:
    task.requested:
      activity:
        id: provision
        tool: infra.provision
        input: {component_id: payload.component_id}
        approval: {decision: provision}
      guard:
        checks: [{id: known_component, check: payload.component_id != ""}]
        on_fail:
          escalate:
            event: task.rejected
            fields: {component_id: payload.component_id}
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
	if handler.Guard == nil || len(handler.Guard.Checks) != 1 || handler.Guard.OnFailSpec.Action != GuardFailureActionEscalate ||
		!handler.Guard.OnFailSpec.Escalation.Fields["component_id"].HasCELValue() {
		t.Fatalf("guard projection lost typed fields: %#v", handler.Guard)
	}
}

func TestProjectNodeControlValuesRejectNestedDuplicate(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("worker:\n  event_handlers:\n    task.requested:\n      activity:\n        approval: {decision: yes, decision: no}\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), "duplicate") || !strings.Contains(err.Error(), "nodes.yaml:5:") {
		t.Fatalf("expected nested duplicate source location, got %v", err)
	}
}
