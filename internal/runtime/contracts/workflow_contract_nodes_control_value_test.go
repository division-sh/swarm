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
		t.Fatalf("guard projection lost typed fields: %#v", handler.Guard)
	}
	// The only guard spelling is a boolean expression; object-form policy and
	// registry references must fail at the same declaration ingress.
	for _, guard := range []string{"{id: ready}", "{check: true}", "{checks: []}", "{on_fail: kill}", "{}", "[]", "null", "''", `"true"`} {
		var h SystemNodeEventHandler
		if err := decodeNodeTestYAML([]byte("guard: "+guard+"\n"), &h); err == nil || !strings.Contains(err.Error(), "guard") {
			t.Fatalf("retired or empty guard %s admitted: %v", guard, err)
		}
	}
	for _, guard := range []string{"true", "false", "payload.ready == true"} {
		var h SystemNodeEventHandler
		if err := decodeNodeTestYAML([]byte("guard: "+guard+"\n"), &h); err != nil || h.Guard == nil || h.Guard.Check != guard {
			t.Fatalf("scalar guard %s lost: %#v %v", guard, h.Guard, err)
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
