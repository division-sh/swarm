package runforkpersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedDeploymentForkIdentityIsInvocationScoped(t *testing.T) {
	source := uuid.NewString()
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 4}
	first := &runfork.ForkOperationRequest{OperationID: uuid.NewString()}
	a, err := selectedRunForkMaterializationID(source, point, first)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := selectedRunForkMaterializationID(source, point, first)
	if err != nil || replay != a {
		t.Fatalf("same invocation changed child identity: %q %q %v", a, replay, err)
	}
	b, err := selectedRunForkMaterializationID(source, point, &runfork.ForkOperationRequest{OperationID: uuid.NewString()})
	if err != nil || b == a {
		t.Fatalf("distinct invocation reused a child: %q %q %v", a, b, err)
	}
	for _, operation := range []*runfork.ForkOperationRequest{nil, {OperationID: "bad"}} {
		if _, err := selectedRunForkMaterializationID(source, point, operation); err == nil {
			t.Fatal("deployment fork without durable invocation was accepted")
		}
	}
	event := runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 1, EventID: uuid.NewString()}
	eventID, err := selectedRunForkMaterializationID(source, event, nil)
	if err != nil || eventID != deterministicRunForkMaterializationID(source, event.EventID) {
		t.Fatalf("ordinary event fork identity changed: %q %v", eventID, err)
	}
}
