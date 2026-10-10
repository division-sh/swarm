package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func nativeMutationLoggingFixture(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) (WorkflowActivityNativeFixtureForTest, context.Context) {
	t.Helper()
	fixture := open(t)
	ctx := runtimeeffects.WithExecutionMode(correlation.WithRunID(fixture.Context, testPipelineRunID), runtimeeffects.ExecutionModeLive)
	if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}
	return fixture, ctx
}

func VerifyUpdateEntityStateLogsMutationRowForStateTransitionForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	f, ctx := nativeMutationLoggingFixture(t, open)
	entity := uuid.NewString()
	pc := f.NewCoordinator(&recordingPipelineBus{}, PipelineCoordinatorOptions{Module: &previewWorkflowModule{
		bundle: lifecycleStateFixtureForTest(t, "mutation-flow", "queued", "done", "flow.transitioned"),
	}})
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{StorageRef: "mutation-flow", EntityID: entity,
		WorkflowName: "mutation-flow", WorkflowVersion: "1.0.0", CurrentState: "queued", StageDefined: true, EntityType: "test_entity"})
	if err := f.Construct(ctx, instance); err != nil {
		t.Fatal(err)
	}
	transitionCtx := nativeMutationTransitionContext(ctx, "mutation-flow", entity)
	event, _ := correlation.InboundEventFromContext(transitionCtx)
	f.PersistEvent(ctx, event)
	if err := persistNativeMutationTransition(t, f, transitionCtx, pc, "mutation-flow"); err != nil {
		t.Fatal(err)
	}
	evidence := requireNativeMutationProjection(t, f, ctx, entity)
	var last *WorkflowMutationRowForTest
	for i := range evidence.Mutations {
		if evidence.Mutations[i].Domain == mutationlog.DomainLifecycleState {
			last = &evidence.Mutations[i]
		}
	}
	if last == nil || last.Path != "" || string(last.OldValue) != `"queued"` || string(last.NewValue) != `"done"` || last.WriterType == "" || last.HandlerStep == "" {
		t.Fatalf("transition lost domain/path, old/new state or attribution: %+v", last)
	}
	assertNativeMutationReconstruction(t, evidence)
}

func VerifyWorkflowInstanceStoreUpsertTracksFieldsGatesAndAccumulatorForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	f, ctx := nativeMutationLoggingFixture(t, open)
	entity := uuid.NewString()
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{StorageRef: "mutation-flow", EntityID: entity,
		WorkflowName: "mutation-flow", WorkflowVersion: "1.0.0", CurrentState: "queued", StageDefined: true, EntityType: "test_entity",
		Fields: map[string]any{"status": "open", "business_status": "pending"}, Gates: map[string]bool{"g_ready": true},
		StateBuckets: map[string]any{"evidence": map[string]any{"score": 1}}})
	if err := f.Construct(ctx, instance); err != nil {
		t.Fatal(err)
	}
	instance = requireNativeMutationInstance(t, f, ctx, "mutation-flow")
	instance.CurrentState = "done"
	instance.EnteredStageAt = time.Now().UTC()
	instance.Fields = map[string]any{"status": "closed", "business_status": "approved"}
	instance.Gates = map[string]bool{"g_done": true}
	instance.StateBuckets = map[string]any{"evidence": map[string]any{"score": 2}, "notes": map[string]any{"count": 1}}
	if err := f.Persistence.store.upsert(ctx, instance); err != nil {
		t.Fatal(err)
	}
	evidence := requireNativeMutationProjection(t, f, ctx, entity)
	requireMutationPaths(t, evidence, "lifecycle_state:", "authored_field:status", "authored_field:business_status", "gate:g_ready", "gate:g_done", "accumulator:evidence", "accumulator:notes")
	assertNativeMutationReconstruction(t, evidence)
}

func VerifyWorkflowInstanceStoreReplaysContainedStateMapListProjectionForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	f, ctx := nativeMutationLoggingFixture(t, open)
	entity := uuid.NewString()
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{StorageRef: "contained-state-flow", EntityID: entity,
		WorkflowName: "contained-state-flow", WorkflowVersion: "1.0.0", CurrentState: "queued", StageDefined: true, EntityType: "test_entity",
		Fields: map[string]any{"verticals": map[string]any{"north": map[string]any{"status": "active", "active_jobs": []any{}}}, "tags": []any{"new"}}})
	if err := f.Construct(ctx, instance); err != nil {
		t.Fatal(err)
	}
	instance = requireNativeMutationInstance(t, f, ctx, "contained-state-flow")
	contained := map[string]any{"north": map[string]any{"status": "busy", "active_jobs": []any{map[string]any{"id": "job-1", "title": "Build"}}}}
	instance.Fields = map[string]any{"verticals": contained, "tags": []any{"new", "vip"}}
	if err := f.Persistence.store.upsert(ctx, instance); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := f.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstance("contained-state-flow"))
	if err != nil || !found || mustCanonicalJSON(t, loaded.Fields["verticals"]) != mustCanonicalJSON(t, contained) || mustCanonicalJSON(t, loaded.Fields["tags"]) != `["new","vip"]` {
		t.Fatalf("contained map/list projection changed: %+v found=%v err=%v", loaded, found, err)
	}
	evidence := requireNativeMutationProjection(t, f, ctx, entity)
	requireMutationPaths(t, evidence, "authored_field:verticals", "authored_field:tags")
	assertNativeMutationReconstruction(t, evidence)
}

func nativeMutationLoggingCoordinator(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) (WorkflowActivityNativeFixtureForTest, context.Context, *PipelineCoordinator, string) {
	t.Helper()
	f, ctx := nativeMutationLoggingFixture(t, open)
	pc := f.NewCoordinator(&recordingPipelineBus{}, PipelineCoordinatorOptions{Module: &previewWorkflowModule{
		bundle: lifecycleStateFixtureForTest(t, ".", "queued", "done", "flow.transitioned"),
	}})
	entity := uuid.NewString()
	if err := f.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{StorageRef: testPipelineRunID, EntityID: entity,
		WorkflowName: ".", WorkflowVersion: "1.0.0", CurrentState: "queued", StageDefined: true, EntityType: "test_entity"})); err != nil {
		t.Fatal(err)
	}
	return f, ctx, pc, entity
}

func VerifyApplyWorkflowGateMutationLogsMutationRowForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	f, ctx, pc, entity := nativeMutationLoggingCoordinator(t, open)
	if err := pc.applyWorkflowGateForTest(ctx, testRunScopedWorkflowInstance(testPipelineRunID).Route, "workflow.ready", "g_ready", false); err != nil {
		t.Fatal(err)
	}
	evidence := requireNativeMutationProjection(t, f, ctx, entity)
	requireMutationPaths(t, evidence, "gate:g_ready")
	assertNativeMutationReconstruction(t, evidence)
}

func VerifyAccumulatorAppendLogsMutationRowForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	f, ctx, pc, entity := nativeMutationLoggingCoordinator(t, open)
	if err := commitAccumulatorAppendForTest(ctx, pc, testRunScopedWorkflowInstance(testPipelineRunID).Route, entity, ".", "research", map[string]any{"summary": "done"}); err != nil {
		t.Fatal(err)
	}
	evidence := requireNativeMutationProjection(t, f, ctx, entity)
	requireMutationPaths(t, evidence, "accumulator:journal")
	assertNativeMutationReconstruction(t, evidence)
}

func VerifyMutationLogSchemaRejectsMissingDomainPathForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	f, ctx, _, entity := nativeMutationLoggingCoordinator(t, open)
	if err := f.TryInvalidMutationPath(ctx, testPipelineRunID, entity); err == nil || !strings.Contains(strings.ToLower(err.Error()), "check") {
		t.Fatalf("malformed accumulator path must hit its database constraint: %v", err)
	}
}

func VerifyQ10NativeFieldlessNoStagePrivateHistoryForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	f, ctx := nativeMutationLoggingFixture(t, open)
	entity := uuid.NewString()
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{StorageRef: "fieldless", EntityID: entity,
		WorkflowName: "fieldless", WorkflowVersion: "1", CurrentState: "pending", StageDefined: false,
		Gates: map[string]bool{"ready": true}, Bookkeeping: map[string]any{"authority": "header"},
		StateBuckets: map[string]any{"count": int64(1)}})
	if err := f.Construct(ctx, instance); err != nil {
		t.Fatal(err)
	}
	evidence := requireNativeMutationProjection(t, f, ctx, entity)
	current := evidence.Current
	if current.CurrentState == nil || *current.CurrentState != "pending" || len(current.Fields) != 0 ||
		current.Gates["ready"] != true || current.Bookkeeping["authority"] != "header" || mustCanonicalJSON(t, current.Accumulator["count"]) != "1" {
		t.Fatalf("fieldless no-stage native construction lost internal/private facts: %+v", current)
	}
	requireMutationPaths(t, evidence, "lifecycle_state:", "gate:ready", "bookkeeping:authority", "accumulator:count")
	assertNativeMutationReconstruction(t, evidence)
}

func VerifyMutationLoggedPipelineWritesFailClosedWithoutHistoryForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	for _, operation := range []string{"state transition", "gate mutation", "accumulator append"} {
		t.Run(operation, func(t *testing.T) {
			f, ctx, pc, entity := nativeMutationLoggingCoordinator(t, open)
			transitionCtx := nativeMutationTransitionContext(ctx, testPipelineRunID, entity)
			event, _ := correlation.InboundEventFromContext(transitionCtx)
			f.PersistEvent(ctx, event)
			if err := f.RemoveMutationLog(ctx); err != nil {
				t.Fatal(err)
			}
			var err error
			switch operation {
			case "state transition":
				err = persistNativeMutationTransition(t, f, transitionCtx, pc, testPipelineRunID)
			case "gate mutation":
				err = pc.applyWorkflowGateForTest(ctx, testRunScopedWorkflowInstance(testPipelineRunID).Route, "workflow.ready", "g_ready", false)
			case "accumulator append":
				err = commitAccumulatorAppendForTest(ctx, pc, testRunScopedWorkflowInstance(testPipelineRunID).Route, entity, ".", "research", map[string]any{"summary": "done"})
			}
			if err == nil || !strings.Contains(err.Error(), "entity_mutations") {
				t.Fatalf("%s must fail on missing history: %v", operation, err)
			}
			current, found, readErr := f.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstance(testPipelineRunID))
			if readErr != nil || !found || current.CurrentState != "queued" || len(current.Gates) != 0 {
				t.Fatalf("failed operation changed current header: %+v found=%v err=%v", current, found, readErr)
			}
			if _, exists := current.StateBuckets["journal"]; exists {
				t.Fatalf("failed operation committed journal: %+v", current.StateBuckets)
			}
		})
	}
}

func persistNativeMutationTransition(t *testing.T, fixture WorkflowActivityNativeFixtureForTest, ctx context.Context, pc *PipelineCoordinator, path string) error {
	t.Helper()
	inbound, ok := correlation.InboundEventFromContext(ctx)
	if !ok {
		return fmt.Errorf("native lifecycle proof requires its persisted inbound event")
	}
	instance := requireNativeMutationInstance(t, fixture, ctx, path)
	current := instance.CurrentState
	transition, err := compiledLifecycleTransitionForTest(pc, instance.WorkflowName, current, "done", string(inbound.Type()))
	if err != nil {
		return err
	}
	route := testRunScopedWorkflowInstance(path).Route
	effect, err := (pipelineWorkflowLifecycleOwner{coordinator: pc}).AcceptedEventEffect(route, identity.NormalizeEntityID(instance.EntityID), inbound, current, "done", transition)
	if err != nil {
		return err
	}
	node, _, found := transition.HandlerOrigin()
	if !found {
		return fmt.Errorf("native lifecycle proof requires its authored handler carrier")
	}
	deliveryRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: node.FlowPath(), FlowInstance: route.InstancePath, EntityID: instance.EntityID,
	})}
	snapshot, err := fixture.AdmitDelivery(ctx, inbound, deliveryRoute)
	if err != nil {
		return err
	}
	if snapshot.Route.Target != deliveryRoute.Target || snapshot.Route.Recipient != deliveryRoute.Recipient || !snapshot.Route.Context.Empty() {
		return fmt.Errorf("native lifecycle delivery differs from the admitted carrier")
	}
	effect, err = effect.WithExecutionOccurrence("delivery", snapshot.DeliveryID)
	if err != nil {
		return err
	}
	instance.CurrentState, instance.EnteredStageAt = "done", inbound.CreatedAt()
	instance.TransitionHistory = append(instance.TransitionHistory, WorkflowTransitionRecord{
		Evidence: *transition, TransitionID: transition.ID(), From: current, To: "done",
		TriggerEventID: inbound.ID(), FiredAt: inbound.CreatedAt(), GuardsEvaluated: transition.GuardsEvaluated(),
	})
	return commitTestWorkflowLifecycleMutation(ctx, pc, route, instance, current, []workflowlifecycle.Effect{effect})
}

func nativeMutationTransitionContext(ctx context.Context, path, entity string) context.Context {
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entity), path)
	event := eventtest.RuntimeControl(uuid.NewString(), "flow.transitioned", "test", "", json.RawMessage(`{}`), 0,
		correlation.RunIDFromContext(ctx), "", envelope, time.Now().UTC())
	return correlation.WithInboundEvent(ctx, event)
}

func requireNativeMutationInstance(t *testing.T, fixture WorkflowActivityNativeFixtureForTest, ctx context.Context, path string) WorkflowInstance {
	t.Helper()
	instance, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstance(path))
	if err != nil || !found {
		t.Fatalf("native constructed header not found: path=%s found=%v err=%v", path, found, err)
	}
	return instance
}

func requireNativeMutationProjection(t *testing.T, fixture WorkflowActivityNativeFixtureForTest, ctx context.Context, entity string) WorkflowMutationProjectionForTest {
	t.Helper()
	evidence, err := fixture.ReadMutationProjection(ctx, correlation.RunIDFromContext(ctx), entity)
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}

func requireMutationPaths(t *testing.T, evidence WorkflowMutationProjectionForTest, paths ...string) {
	t.Helper()
	seen := map[string]bool{}
	for _, row := range evidence.Mutations {
		seen[string(row.Domain)+":"+row.Path] = true
	}
	for _, path := range paths {
		if !seen[path] {
			t.Fatalf("missing mutation %s in %v", path, seen)
		}
	}
}

func assertNativeMutationReconstruction(t *testing.T, evidence WorkflowMutationProjectionForTest) {
	t.Helper()
	if len(evidence.Mutations) == 0 {
		t.Fatal("entity_mutations is empty; canonical mutation surface is missing")
	}
	var records []mutationlog.ProjectionMutation
	for _, row := range evidence.Mutations {
		var value any
		if len(row.NewValue) > 0 {
			if err := canonicaljson.DecodePreservingNumberLexemes(row.NewValue, &value); err != nil {
				t.Fatal(err)
			}
		}
		records = append(records, mutationlog.ProjectionMutation{Domain: row.Domain, Path: row.Path, NewValue: value})
	}
	actual, err := mutationlog.ReconstructEntityStateProjection(records)
	if err != nil {
		t.Fatal(err)
	}
	if mustCanonicalJSON(t, actual) != mustCanonicalJSON(t, evidence.Current) {
		t.Fatalf("mutation reconstruction mismatch:\n got=%s\nwant=%s", mustCanonicalJSON(t, actual), mustCanonicalJSON(t, evidence.Current))
	}
}

func mustCanonicalJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(fmt.Errorf("marshal mutation projection: %w", err))
	}
	return string(raw)
}
