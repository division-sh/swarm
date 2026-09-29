package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeDeclarationsValueReachesNestedHandler(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("worker:\n  execution_type: system_node\n  event_handlers:\n    task.requested:\n      emit:\n        event: task.completed\n        fields: {answer: '${payload.answer}'}\n"))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	handler := nodes["worker"].EventHandlers["task.requested"]
	if handler.Emit.Event != "task.completed" || !handler.Emit.Fields["answer"].HasCELValue() {
		t.Fatalf("wrong node-root projection: %#v", handler)
	}
	retired, err := yamlsource.Load([]byte("worker:\n  event_handlers:\n    task.requested:\n      from: null\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(retired.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), "nodes.yaml:4:") || !strings.Contains(err.Error(), "from") {
		t.Fatalf("expected nested source-located retirement, got %v", err)
	}
}
