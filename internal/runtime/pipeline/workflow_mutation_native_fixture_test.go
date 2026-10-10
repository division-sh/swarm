package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

type WorkflowMutationNativeFixtureForTest struct {
	WorkflowProjectionNativeFixtureForTest
	NewCoordinator  func(Bus, PipelineCoordinatorOptions) *PipelineCoordinator
	PublishAndClaim func(context.Context, events.Event, events.DeliveryRoute) (deliverylifecycle.ClaimedObligation, error)
}

func prepareClaimedWorkflowTransitionForTest(t *testing.T, fixture WorkflowMutationNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, route flowidentity.Route, entityID, from, to, eventType string) (context.Context, deliverylifecycle.ClaimedObligation) {
	t.Helper()
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), route.InstancePath)
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType(strings.TrimSpace(eventType)), "test", "", []byte(`{}`), 0,
		correlation.RunIDFromContext(ctx), envelope, time.Now().UTC())
	transitionCtx := correlation.WithInboundEvent(ctx, event)
	inbound, found := correlation.InboundEventFromContext(transitionCtx)
	if !found {
		t.Fatal("native transition has no exact inbound event")
	}
	transition, err := compiledLifecycleTransitionForTest(pc, "mutation-flow", from, to, eventType)
	if err != nil {
		t.Fatal(err)
	}
	node, _, found := transition.HandlerOrigin()
	if !found {
		t.Fatal("native transition has no exact declared handler")
	}
	target := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(node),
		Target:    events.MustExistingEntityTarget(events.RouteIdentity{FlowID: node.FlowPath(), FlowInstance: route.InstancePath, EntityID: entityID}),
	}
	claimed, err := fixture.PublishAndClaim(transitionCtx, inbound, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := claimed.Claim.Validate(); err != nil {
		t.Fatal(err)
	}
	if claimed.Snapshot.Route.Target != target.Target || claimed.Snapshot.Route.Recipient != target.Recipient || !claimed.Snapshot.Route.Context.Empty() || claimed.Snapshot.EventID != inbound.ID() || claimed.Snapshot.DeliveryID != claimed.Claim.DeliveryID() {
		t.Fatal("native transition acquired a different delivery")
	}
	return deliverylifecycle.WithClaim(transitionCtx, claimed.Claim), claimed
}

func persistClaimedWorkflowStateForTest(ctx context.Context, pc *PipelineCoordinator, route flowidentity.Route, entityID, nextState, eventType string, claimed deliverylifecycle.ClaimedObligation) error {
	return pc.persistWorkflowStateWithAdmissionForTest(ctx, route, entityID, nextState, eventType, func(ctx context.Context, _ *PipelineCoordinator, effect workflowlifecycle.Effect) (workflowlifecycle.Effect, error) {
		active, activeFound := deliverylifecycle.ClaimFromContext(ctx)
		event, found := correlation.InboundEventFromContext(ctx)
		if !found || !activeFound || !active.Same(claimed.Claim) || claimed.Snapshot.EventID != event.ID() || claimed.Claim.RunID() != event.RunID() || claimed.Snapshot.Route.Target.Route() != (events.RouteIdentity{FlowID: "mutation-flow", FlowInstance: route.InstancePath, EntityID: identity.NormalizeEntityID(entityID).String()}) {
			return effect, fmt.Errorf("native transition claim differs from exact event/run/route")
		}
		return effect.WithExecutionOccurrence("delivery", claimed.Claim.DeliveryID())
	})
}

func nativeWorkflowRevisionConflictForTest(err error) bool {
	failure, ok := failures.As(err)
	return failures.IsStateContention(err) && ok && failure.Failure.Class == failures.ClassLifecycleConflict && failure.Failure.Detail.Code == "workflow_engine_state_revision_conflict" &&
		failure.Failure.Detail.Attributes["route"] == "mutation-flow" && failure.Failure.Detail.Attributes["expected_state"] == "queued" && failure.Failure.Detail.Attributes["expected_revision"] == int64(1)
}

func TestNativeWorkflowRevisionConflictRejectsUntypedWrongCodeScopeAndRevision(t *testing.T) {
	valid := func(class failures.Class, code, route, state string, revision int64) error {
		return failures.New(class, code, "workflow-engine-persistence", "commit_state", map[string]any{
			"route": route, "expected_state": state, "expected_revision": revision,
		})
	}
	if !nativeWorkflowRevisionConflictForTest(valid(failures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "mutation-flow", "queued", 1)) {
		t.Fatal("canonical conflict refused")
	}
	for _, err := range []error{
		nil, errors.New("changed before commit"),
		errors.Join(valid(failures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "mutation-flow", "queued", 1), errors.New("independent failure")),
		valid(failures.ClassInternalFailure, "workflow_engine_state_revision_conflict", "mutation-flow", "queued", 1),
		valid(failures.ClassLifecycleConflict, "other", "mutation-flow", "queued", 1),
		valid(failures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "other", "queued", 1),
		valid(failures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "mutation-flow", "done", 1),
		valid(failures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "mutation-flow", "queued", 2),
	} {
		if nativeWorkflowRevisionConflictForTest(err) {
			t.Fatalf("noncanonical conflict accepted: %v", err)
		}
	}
}

func VerifyNativeWorkflowMutationTransitionRefusesMissingOrForeignClaimForTest(t *testing.T, open func(*testing.T, string) WorkflowMutationNativeFixtureForTest) {
	fixture := open(t, testPipelineRunID)
	entityID := uuid.NewString()
	seedWorkflowInstanceForMutationTest(t, fixture, entityID)
	pc := fixture.NewCoordinator(&recordingPipelineBus{}, PipelineCoordinatorOptions{
		Module: &previewWorkflowModule{bundle: lifecycleStateFixtureForTest(t, "mutation-flow", "queued", "done", "workflow.completed")},
	})
	route := testWorkflowInstanceRoute("mutation-flow")
	ctx, claimed := prepareClaimedWorkflowTransitionForTest(t, fixture, pc, fixture.Context, route, entityID, "queued", "done", "workflow.completed")
	foreign := claimed
	foreign.Snapshot.EventID = uuid.NewString()
	for _, attempt := range []struct {
		ctx   context.Context
		claim deliverylifecycle.ClaimedObligation
	}{
		{deliverylifecycle.WithoutClaim(ctx), claimed},
		{ctx, foreign},
		{ctx, deliverylifecycle.ClaimedObligation{}},
	} {
		err := persistClaimedWorkflowStateForTest(attempt.ctx, pc, route, entityID, "done", "workflow.completed", attempt.claim)
		if err == nil || !strings.Contains(err.Error(), "native transition claim differs") {
			t.Fatalf("unauthorized transition: %v", err)
		}
		current, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstance("mutation-flow"))
		if err != nil || !found || current.CurrentState != "queued" || current.Revision != 1 || len(current.TransitionHistory) != 0 {
			t.Fatalf("refused claim changed native state: %+v found=%v err=%v", current, found, err)
		}
	}
	if err := persistClaimedWorkflowStateForTest(ctx, pc, route, entityID, "done", "workflow.completed", claimed); err != nil {
		t.Fatal(err)
	}
	current, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstance("mutation-flow"))
	if err != nil || !found || current.CurrentState != "done" || len(current.TransitionHistory) != 1 {
		t.Fatalf("actual native claim did not authorize the transition: %+v found=%v err=%v", current, found, err)
	}
}
