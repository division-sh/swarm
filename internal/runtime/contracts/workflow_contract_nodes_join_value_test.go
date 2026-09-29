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

func TestW4DeliveryJoinForbiddenFieldPresence(t *testing.T) {
	const base = "id: delivered\nmembers: {from_fan_out: true}\non_complete: {emit: batch.completed}\n"
	for _, field := range []string{"stage", "window", "output", "complete_when", "remaining", "timeout", "members.from", "members.by"} {
		t.Run(field, func(t *testing.T) {
			for _, shape := range nodePresenceShapes("waiting", "[waiting]", "{from: entity.batch, by: payload.batch}") {
				t.Run(shape.name, func(t *testing.T) {
					body := base
					if shape.name != "missing" {
						if member, found := strings.CutPrefix(field, "members."); found {
							body = strings.Replace(body, "members: {from_fan_out: true}", "members:\n  from_fan_out: true\n  "+member+": "+shape.value, 1)
						} else {
							body += field + ": " + shape.value + "\n"
						}
					}
					var join JoinSpec
					err := decodeNodeTestYAML([]byte(body), &join)
					if shape.name == "missing" {
						if err != nil || !join.IsFanOutDeliveryBarrier() || join.ID != "delivered" || join.OnComplete.Emit.Event != "batch.completed" {
							t.Fatalf("valid delivery join lost its typed facts: %#v, %v", join, err)
						}
					} else if err == nil {
						t.Fatalf("delivery join admitted forbidden %s in %s state: %#v", field, shape.name, join)
					}
				})
			}
		})
	}
}

func TestW4ArrivalJoinNullTimeoutRemainsOmitted(t *testing.T) {
	var join JoinSpec
	if err := decodeNodeTestYAML([]byte("stage: awaiting\nmembers: {from: entity.ids, by: payload.id}\ntimeout: null\n"), &join); err != nil {
		t.Fatal(err)
	}
	if join.Mode() != WorkflowJoinModeArrival || join.TimeoutFound || !join.timeoutFound || join.Members.By != "payload.id" || !join.Members.BySet {
		t.Fatalf("arrival mode/null timeout meaning changed: %#v", join)
	}
}
