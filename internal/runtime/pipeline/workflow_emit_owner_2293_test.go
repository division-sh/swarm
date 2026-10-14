package pipeline

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/engine"
)

func TestCanonicalGuardEmitReaderRetainsPreWriteAssignmentPoint(t *testing.T) {
	handler := contracts.SystemNodeEventHandler{Guard: &contracts.GuardSpec{
		Check: "payload.score > 0",
		OnFailSpec: contracts.GuardFailureSpec{Action: contracts.GuardFailureActionEscalate,
			Escalation: contracts.EmitSpec{Event: "work.escalated", Fields: map[string]contracts.ExpressionValue{
				"score": contracts.CELExpression("entity.score"),
			}},
		},
	}}
	readers := WorkflowHandlerEmitExpressions(nil, identitytest.RootNode(t, "worker"), "work.requested", handler)
	if len(readers) != 1 {
		t.Fatalf("canonical guard emit produced %d readers, want exactly 1", len(readers))
	}
	reader := readers[0]
	if reader.Kind != "guard escalation emit field score" || reader.SourceSlot != "guard.on_fail.escalate.fields.score" || reader.Expression != "entity.score" || reader.Phase != WorkflowEntityFieldLifecycleGuardEscalation || reader.EmitSite == nil || reader.EmitSite.Source != "handler.guard.on_fail.escalate" {
		t.Fatalf("guard emit reader phase/source changed: %#v", reader)
	}
	point, err := entityAssignmentReaderPoint(handler, reader)
	if err != nil {
		t.Fatal(err)
	}
	want := engine.EntityAssignmentPoint{Step: engine.StepGuard, RuleIndex: -1}
	if !reflect.DeepEqual(point, want) {
		t.Fatalf("guard emit must read before handler writes: point=%#v, want=%#v", point, want)
	}
}
