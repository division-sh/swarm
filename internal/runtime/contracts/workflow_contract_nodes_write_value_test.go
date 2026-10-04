package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeDataAccumulationValue(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte(`worker:
  event_handlers:
    task.requested:
      data_accumulation:
        source_event: task.requested
        writes:
          - name
          - target_field: dispatch_count
            value: payload.count
          - op: clear
            target: entity.old_value
`))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	accumulation := nodes["worker"].EventHandlers["task.requested"].DataAccumulation
	if accumulation.SourceEvent != "task.requested" || len(accumulation.Writes) != 3 ||
		accumulation.Writes[0].Field != "name" || accumulation.Writes[1].Value.CEL != "payload.count" ||
		accumulation.Writes[2].Operation != WorkflowDataOperationClear {
		t.Fatalf("write forms not preserved: %#v", accumulation)
	}
}

func TestProjectNodeDataAccumulationValueRejectsRetiredExpression(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("worker:\n  event_handlers:\n    task.requested:\n      data_accumulation:\n        writes: [{target_field: name, expression: payload.name}]\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), "is not supported") || !strings.Contains(err.Error(), "nodes.yaml:5:") {
		t.Fatalf("expected source-located write retirement, got %v", err)
	}
}
