package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func newFanOutConstructorFixture(t *testing.T, backend string) receiverConfigActivationFixture {
	t.Helper()
	files := map[string]string{"schema.yaml": "name: fan-out-store-fixture\n"}
	for _, flow := range []string{"completion_one", "completion_two"} {
		files[flow+"/schema.yaml"] = "name: " + flow + "\nstages:\n  pending: {}\n"
		files[flow+"/events.yaml"] = "items.child:\n  ordinal: integer\n"
		files[flow+"/nodes.yaml"] = "completion:\n  execution_type: system_node\n  event_handlers:\n    items.child: {}\n"
	}
	return newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
}

// The barrier tests exercise commit arbitration, not construction. Their
// receivers nevertheless enter through the real constructor before mutation.
func constructFanOutCompletionReceiver(t *testing.T, ctx context.Context, f receiverConfigActivationFixture, flow string, at time.Time) (events.DeliveryRoute, pipeline.WorkflowEngineStateRecord) {
	t.Helper()
	req := sqliteFlowActivationRequest(f.bundle, flow, flow, "", flow)
	runID := correlation.RunIDFromContext(ctx)
	rootIdentity := flowidentity.Stored(semanticview.Wrap(f.bundle), ".", runID, runID, runID, "")
	childIdentity, err := flowidentity.KeylessChild(semanticview.Wrap(f.bundle), rootIdentity, flow)
	if err != nil {
		t.Fatal(err)
	}
	req.Instance = childIdentity
	req.OccurredAt = at
	plan, err := f.manager.PrepareFlowInstanceActivation(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct completion receiver: result=%+v err=%v", committed, err)
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(correlation.RunIDFromContext(ctx), req.Instance.Route())
	if err != nil {
		t.Fatal(err)
	}
	instance, found, err := f.workflows.Load(ctx, owner)
	if err != nil || !found || instance.EntityID != req.Instance.EntityID || instance.Revision != 1 {
		t.Fatalf("constructed completion readback: instance=%+v found=%t err=%v", instance, found, err)
	}
	node, err := identity.AdmitExecutableNodeDeclaration(flow, "completion")
	if err != nil {
		t.Fatal(err)
	}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: flow, FlowInstance: owner.Route.InstancePath, EntityID: instance.EntityID,
	})}
	state := pipeline.WorkflowEngineStateRecord{
		Identity: owner, EntityID: instance.EntityID, WorkflowName: instance.WorkflowName, WorkflowVersion: instance.WorkflowVersion,
		Mode: instance.Mode, Status: instance.Status, CurrentState: instance.CurrentState, StageDefined: instance.StageDefined,
		Fields: []byte(`{}`), Bookkeeping: []byte(`{}`), Gates: []byte(`{}`), Accumulator: []byte(`{}`), Config: []byte(`{}`), InitialFields: []byte(`{}`),
		EnteredStageAt: instance.EnteredStageAt, CreatedAt: instance.CreatedAt, UpdatedAt: at.Add(time.Second),
		ExpectedState: instance.CurrentState, ExpectedRevision: instance.Revision, Transition: pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion,
	}
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
	return route, state
}
