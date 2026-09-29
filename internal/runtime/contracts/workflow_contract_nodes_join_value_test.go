package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeJoinValuePreservesFanOutMode(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte(`worker:
  event_handlers:
    task.ready:
      join:
        id: collected
        members: {from_fan_out: true}
        on_complete: {emit: task.collected}
`))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	join := nodes["worker"].EventHandlers["task.ready"].Join
	if join == nil || !join.IsFanOutDeliveryBarrier() || !join.OnCompleteFound || join.OnComplete.Emit.Event != "task.collected" {
		t.Fatalf("fan-out join projection lost mode or outcome: %#v", join)
	}
}

func TestProjectNodeJoinValueRejectsArrivalFieldsInFanOutMode(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("worker:\n  event_handlers:\n    task.ready:\n      join: {id: collected, stage: waiting, members: {from_fan_out: true}, on_complete: {emit: task.collected}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), "forbids arrival-only fields") || !strings.Contains(err.Error(), "nodes.yaml:4:") {
		t.Fatalf("expected source-located fan-out shape rejection, got %v", err)
	}
}
