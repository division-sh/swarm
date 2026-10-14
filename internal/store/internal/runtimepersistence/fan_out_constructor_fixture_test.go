package runtimepersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func newFanOutConstructorFixture(t *testing.T, backend string) receiverConfigActivationFixture {
	t.Helper()
	files := map[string]string{"schema.yaml": "name: fan-out-store-fixture\nstages:\n  work: {}\n"}
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
	source := semanticview.Wrap(f.bundle)
	rootIdentity := flowidentity.Stored(source, ".", runID, runID, runID, "")
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found {
		t.Fatal("completion construction requires its exact source fact")
	}
	reader := f.store.(pipeline.FlowInstanceIndexReader)
	rootOwner, err := flowidentity.NewRunScopedFlowInstance(runID, rootIdentity.Route())
	if err != nil {
		t.Fatal(err)
	}
	rootLookup, err := pipeline.NewExactFlowInstanceLookup(source, fact, rootOwner)
	if err != nil {
		t.Fatal(err)
	}
	root, found, err := reader.LookupFlowInstance(ctx, rootLookup)
	if err != nil {
		t.Fatalf("observe exact completion parent: %v", err)
	}
	if !found {
		rootRequest := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
		rootRequest.Instance, rootRequest.OccurredAt = rootIdentity, at
		rootPlan, err := f.manager.PrepareFlowInstanceActivation(ctx, rootRequest)
		if err != nil {
			t.Fatal(err)
		}
		committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, rootPlan)
		if err != nil || !committed.Acknowledged || !committed.Created {
			t.Fatalf("construct complete completion parent tree: result=%+v err=%v", committed, err)
		}
		root, found, err = reader.LookupFlowInstance(ctx, rootLookup)
		if err != nil || !found {
			t.Fatalf("read acknowledged completion parent: found=%t err=%v", found, err)
		}
	}
	childIdentity, err := flowidentity.KeylessChild(source, root.Identity(), flow)
	if err != nil {
		t.Fatal(err)
	}
	req.Instance = childIdentity
	req.OccurredAt = at
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, childIdentity.Route())
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := pipeline.NewExactFlowInstanceLookup(source, fact, owner)
	if err != nil {
		t.Fatal(err)
	}
	child, found, err := reader.LookupFlowInstance(ctx, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		plan, err := f.manager.PrepareFlowInstanceActivation(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, plan)
		if err != nil || !committed.Acknowledged || !committed.Created {
			t.Fatalf("construct completion receiver: result=%+v err=%v", committed, err)
		}
		child, found, err = reader.LookupFlowInstance(ctx, lookup)
		if err != nil || !found {
			t.Fatalf("read acknowledged completion receiver: found=%t err=%v", found, err)
		}
	}
	owner.Route = child.Identity().Route()
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
	header, err := pipeline.WorkflowInstanceHeaderPayloadForIdentity(child.Identity(), instance.WorkflowVersion)
	if err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	state := pipeline.WorkflowEngineStateRecord{
		Identity: owner, EntityID: instance.EntityID, WorkflowName: instance.WorkflowName, WorkflowVersion: instance.WorkflowVersion,
		ParentInstance: child.Identity().ParentRoute.FlowInstance,
		Mode:           instance.Mode, Status: instance.Status, CurrentState: instance.CurrentState, StageDefined: instance.StageDefined,
		Fields: []byte(`{}`), Bookkeeping: []byte(`{}`), Gates: []byte(`{}`), Accumulator: []byte(`{}`), Config: config, InitialFields: []byte(`{}`),
		EnteredStageAt: instance.EnteredStageAt, CreatedAt: instance.CreatedAt, UpdatedAt: at.Add(time.Second),
		ExpectedState: instance.CurrentState, ExpectedRevision: instance.Revision, Transition: pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion,
	}
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
	return route, state
}

func seedConstructedFanOutBarrierLoopForTest(t *testing.T, ctx context.Context, f receiverConfigActivationFixture, fixture fanOutOwnerFixture, at time.Time) (timeridentity.TimerHandle, string, loopruntime.Activation) {
	t.Helper()
	rootIdentity := flowidentity.Stored(semanticview.Wrap(f.bundle), ".", fixture.runID, fixture.runID, fixture.runID, "")
	owner, err := flowidentity.NewRunScopedFlowInstance(fixture.runID, rootIdentity.Route())
	if err != nil {
		t.Fatal(err)
	}
	root, found, err := f.workflows.Load(ctx, owner)
	if err != nil || !found || root.EntityID != rootIdentity.EntityID || root.CurrentState != "work" || !root.StageDefined {
		t.Fatalf("constructed barrier loop owner: instance=%+v found=%t err=%v", root, found, err)
	}
	activation, err := loopruntime.New(fixture.runID, root.EntityID, ".", "retry", "revision_id", fixture.eventID, "work", 3, at)
	if err != nil {
		t.Fatal(err)
	}
	handle := seedFanOutDeliveryBarrierRecord(t, ctx, f.db, fixture, root.EntityID, activation.Generation(), at, owner.Route)
	updateFanOutBarrierLoopState(t, ctx, f.db, fixture.runID, root.EntityID, activation, at)
	return handle, root.EntityID, activation
}
