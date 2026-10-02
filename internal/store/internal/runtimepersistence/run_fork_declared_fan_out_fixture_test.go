package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

// Fork consumers need a real original declaration, unlike isolated SQL-owner
// fixtures. The capsule is explicit; claim, intent and settlement use real owners.
func seedDeclaredForkFanOutFixture(t *testing.T, backend string, fixture authorActivityReceiptFixture, cardinality int, at time.Time) (context.Context, fanOutOwnerFixture) {
	t.Helper()
	ctx, source, _ := seedDeclaredForkFanOutFixtureWithBarrier(t, backend, fixture, cardinality, at, false)
	return ctx, source
}

func seedDeclaredForkFanOutFixtureWithBarrier(t *testing.T, backend string, fixture authorActivityReceiptFixture, cardinality int, at time.Time, withBarrier bool) (context.Context, fanOutOwnerFixture, timeridentity.TimerHandle) {
	ctx, source, handle, _ := seedDeclaredForkFanOutGenerationFixture(t, backend, fixture, cardinality, at, withBarrier, false)
	return ctx, source, handle
}

func seedDeclaredForkFanOutGenerationFixture(t *testing.T, backend string, fixture authorActivityReceiptFixture, cardinality int, at time.Time, withBarrier, withGeneration bool) (context.Context, fanOutOwnerFixture, timeridentity.TimerHandle, func(bool)) {
	return seedDeclaredForkFanOutGenerationFromSource(t, backend, fixture, cardinality, at, withBarrier, withGeneration, canonicalrouting.CopyForkFanOutCarrier(t, withGeneration, withBarrier), nil, nil)
}

func seedDeclaredNumericForkFanOutFixture(t *testing.T, backend string, fixture authorActivityReceiptFixture, cardinality int, at time.Time, resourceRows bool) (context.Context, fanOutOwnerFixture) {
	t.Helper()
	var rows any
	if resourceRows {
		items := make([]map[string]any, cardinality)
		for i := range items {
			items[i] = map[string]any{"slug": fmt.Sprintf("item-%03d", i), "score": int64(i + 1)}
		}
		rows = items
	}
	ctx, source, _, _ := seedDeclaredForkFanOutGenerationFromSource(t, backend, fixture, cardinality, at, false, false,
		canonicalrouting.CopyNumericForkFanOutCarrier(t, resourceRows), map[string]any{"integer": int64(75), "decimal": float64(75)}, rows)
	return ctx, source
}

func seedDeclaredEntityForkFanOutFixture(t *testing.T, backend string, fixture authorActivityReceiptFixture, cardinality int, at time.Time) (context.Context, fanOutOwnerFixture) {
	t.Helper()
	root := canonicalrouting.CopyForkFanOutCarrier(t, false, false)
	if err := os.WriteFile(filepath.Join(root, "entities.yaml"), []byte("root:\n  items: '[text]'\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	nodesPath := filepath.Join(root, "nodes.yaml")
	nodes, err := os.ReadFile(nodesPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(nodes), "items_from: payload.items") != 1 {
		t.Fatal("entity source fixture requires exactly one payload source site")
	}
	if err := os.WriteFile(nodesPath, []byte(strings.Replace(string(nodes), "items_from: payload.items", "items_from: entity.items", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	items := make([]string, cardinality)
	for i := range items {
		items[i] = fmt.Sprintf("item-%03d", i)
	}
	ctx, source, _, _ := seedDeclaredForkFanOutGenerationFromSource(t, backend, fixture, cardinality, at, false, false, root, map[string]any{"items": items}, nil)
	return ctx, source
}

func seedDeclaredForkFanOutGenerationFromSource(t *testing.T, backend string, fixture authorActivityReceiptFixture, cardinality int, at time.Time, withBarrier, withGeneration bool, root string, captured map[string]any, rows any) (context.Context, fanOutOwnerFixture, timeridentity.TimerHandle, func(bool)) {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	runID := uuid.NewString()
	ctx := correlation.WithRunID(seedSelectedActivitySourceRun(t, fixture, runID, source), runID)
	construction := sqliteFlowActivationRequest(bundle, ".", runID, "", runID)
	construction.Instance = flowidentity.Stored(source, ".", runID, runID, runID, "")
	construction.OccurredAt = at.Add(-2 * time.Second)
	constructed := constructHistoricalSourceFixture(t, ctx, fixture.store.(agentFixtureFlowStore), construction)
	persisted, err := constructed.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	record := persisted.State
	currentRevision := int64(1)
	record.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	selected := fixture.store.(storeTestDurableEventBusStore)
	node := mustPersistenceRootNode("fan-out-source")
	plans := source.FanOutPlansForHandler(node, "items.ready")
	if len(plans) != 1 {
		t.Fatal("fixture requires exactly one compiled fan-out")
	}
	items := make([]string, cardinality)
	for i := range items {
		items[i] = fmt.Sprintf("item-%03d", i)
	}
	generation := attemptgeneration.Generation{}
	var activation loopruntime.Activation
	payload := map[string]any{"items": items}
	if rows != nil {
		payload["items"] = rows
	}
	if withGeneration {
		start := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "review.start", "fan-out-test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at.Add(-time.Second))
		controller := mustPersistenceRootNode("loop-controller")
		startRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(controller), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})}
		if err := commitSemanticEventFixtureWithRoutes(ctx, selected, start, []events.DeliveryRoute{startRoute}); err != nil {
			t.Fatal(err)
		}
		startClaim, err := claimDeliveryFixture(ctx, selected, start, startRoute)
		if err != nil {
			t.Fatal(err)
		}
		activation, err = loopruntime.New(runID, runID, ".", "revision", "revision_id", start.ID(), "review", 3, start.CreatedAt())
		if err != nil {
			t.Fatal(err)
		}
		record.ExpectedState, record.ExpectedRevision = record.CurrentState, currentRevision
		record.CurrentState, record.UpdatedAt, record.EnteredStageAt = "review", start.CreatedAt(), start.CreatedAt()
		entry := forkFanOutAdmittedStageEntry(t, bundle, &record, start, controller, startClaim.Claim.DeliveryID())
		buckets := map[string]map[string]any{}
		if err := loopruntime.Store(buckets, activation); err != nil {
			t.Fatal(err)
		}
		record.Accumulator = json.RawMessage(forkTestJSON(t, buckets))
		if _, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{
			State: record, Lifecycle: pipeline.WorkflowLifecycleMutationPlan{StageEntry: entry},
			DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: startClaim.Claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()},
		}); err != nil {
			t.Fatal(err)
		}
		currentRevision++
		generation = activation.Generation()
		payload["revision_id"] = generation.RevisionID
	}
	trigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "items.ready", "fan-out-test", "", []byte(forkTestJSON(t, payload)), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at)
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})}
	if err := commitSemanticEventFixtureWithRoutes(ctx, selected, trigger, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claim, err := claimDeliveryFixture(ctx, selected, trigger, route)
	if err != nil {
		t.Fatal(err)
	}
	request := fanoutobligation.IntentRequest{
		Key: fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: claim.Claim.DeliveryID(), ElementRef: plans[0].Ref.ElementRef}, PlanRef: plans[0].Ref,
		Source: fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, EventID: trigger.ID(), Field: "items"}, Cardinality: cardinality,
		Capsule: fanoutobligation.Capsule{NodeKey: node.Key(), ExecutionFlowID: ".", Route: flowidentity.StoredRoute(".", runID, runID), EntityID: runID,
			SourceProjection: plans[0].SemanticEvidence(),
			HandlerEventKey:  "items.ready", CurrentState: record.CurrentState, ProducerSource: trigger.RoutingSource(), Receiver: &fanoutobligation.ExecutionReceiver{Node: node, Target: route.Target}, Lineage: events.LineageFromEvent(trigger)},
	}
	if plans[0].ItemsFrom == "entity.items" {
		request.Source = fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEntityField, RunID: runID, EntityID: runID, Field: "items"}
	}
	record.ExpectedState, record.ExpectedRevision, record.UpdatedAt = record.CurrentState, currentRevision, at
	if captured != nil {
		request.Capsule.Entity, request.Capsule.StateFields = captured, captured
		record.Fields, err = canonicaljson.MarshalPreservingNumberKinds(captured)
		if err != nil {
			t.Fatal(err)
		}
	}
	if withGeneration {
		request.Capsule.Loop = activation.Context()
		buckets := map[string]map[string]any{}
		if err := loopruntime.Store(buckets, activation); err != nil {
			t.Fatal(err)
		}
		record.Accumulator = json.RawMessage(forkTestJSON(t, buckets))
	}
	var barrier *fanoutbarrier.Registration
	var handle timeridentity.TimerHandle
	if withBarrier {
		joinPlan, ok := semanticview.WorkflowJoinPlanForHandler(source, node, "items.ready")
		if !ok {
			t.Fatal("fixture requires the compiled fan-out join")
		}
		declaration, err := request.PlanRef.ElementRef.DeclarationIdentity()
		if err != nil {
			t.Fatal(err)
		}
		join, err := timeridentity.NewFanOutDeliveryJoinRef(node, "items.ready", joinPlan.Spec.ID, declaration, request.PlanRef.BundleHash, request.PlanRef.SemanticDigest)
		if err != nil {
			t.Fatal(err)
		}
		join, err = join.BindFanOutIntent(claim.Claim.DeliveryID(), generation)
		if err != nil {
			t.Fatal(err)
		}
		handle, err = timeridentity.JoinCompleteHandle(join)
		if err != nil {
			t.Fatal(err)
		}
		barrier = &fanoutbarrier.Registration{IntentKey: request.Key, PlanRef: request.PlanRef, Handle: handle,
			Route: request.Capsule.Route, EntityID: runID, RoutingSource: trigger.RoutingSource(), ExecutionMode: trigger.ExecutionMode(), CreatedAt: at}
	}
	if _, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{
		State: record, FanOutIntent: &request, FanOutBarrier: barrier,
		DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claim.Claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()},
	}); err != nil {
		t.Fatal(err)
	}
	// Advance the persisted owner only after barrier closure/terminal evidence has
	// been written, leaving the registration's old attempt intact at revision R.
	advance := func(closeLoop bool) {
		var revision int64
		if err := fixture.db.QueryRowContext(ctx, `SELECT revision FROM flow_instances WHERE run_id=$1 AND entity_id=$1`, runID).Scan(&revision); err != nil {
			t.Fatal(err)
		}
		state := "review"
		eventType := "review.retry"
		if closeLoop {
			state = "done"
			eventType = "review.closed"
		}
		event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(eventType), "fan-out-test", "", []byte(forkTestJSON(t, map[string]any{"revision_id": activation.Generation().RevisionID})), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at.Add(2*time.Second))
		controller := mustPersistenceRootNode("loop-controller")
		controllerRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(controller), Target: route.Target}
		if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{controllerRoute}); err != nil {
			t.Fatal(err)
		}
		controllerClaim, err := claimDeliveryFixture(ctx, selected, event, controllerRoute)
		if err != nil {
			t.Fatal(err)
		}
		if closeLoop {
			err = activation.Close(state, event.ID(), event.CreatedAt())
		} else {
			_, err = activation.Repeat(state, event.ID(), event.CreatedAt())
		}
		if err != nil {
			t.Fatal(err)
		}
		buckets := map[string]map[string]any{}
		if err := loopruntime.Store(buckets, activation); err != nil {
			t.Fatal(err)
		}
		next := record
		next.ExpectedState, next.ExpectedRevision, next.CurrentState = "review", revision, state
		next.EnteredStageAt, next.UpdatedAt = event.CreatedAt(), event.CreatedAt()
		entry := forkFanOutAdmittedStageEntry(t, bundle, &next, event, controller, controllerClaim.Claim.DeliveryID())
		next.Accumulator = json.RawMessage(forkTestJSON(t, buckets))
		if _, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{
			State: next, Lifecycle: pipeline.WorkflowLifecycleMutationPlan{StageEntry: entry},
			DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: controllerClaim.Claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()},
		}); err != nil {
			t.Fatal(err)
		}
	}
	return ctx, fanOutOwnerFixture{runID: runID, eventID: trigger.ID(), deliveryID: claim.Claim.DeliveryID(), flowPath: plans[0].Ref.ElementRef.FlowPath,
		semanticPath: plans[0].Ref.ElementRef.SemanticPath, createdAt: at, bundleHash: bundle.SourceArtifact.BundleHash(), artifact: bundle.SourceArtifact, plan: plans[0]}, handle, advance
}

func forkFanOutAdmittedStageEntry(t *testing.T, bundle *contracts.WorkflowContractBundle, record *pipeline.WorkflowEngineStateRecord, event events.Event, node identity.ExecutableNode, deliveryID string) *timeridentity.StageEntryRef {
	t.Helper()
	graph, found := bundle.WorkflowStageTopology(".")
	if !found {
		t.Fatal("fan-out fixture lost compiled root lifecycle")
	}
	var site *contracts.WorkflowTransitionSite
	for _, edge := range graph.Edges {
		if !edge.Node.Equal(node) || edge.EventType != string(event.Type()) || edge.From != record.ExpectedState || edge.To != record.CurrentState {
			continue
		}
		if site != nil {
			t.Fatal("fan-out fixture requires one exact compiled transition site")
		}
		value := edge.Site()
		site = &value
	}
	if site == nil {
		t.Fatalf("fan-out declaration has no transition for %s %s: %s -> %s", node.Key(), event.Type(), record.ExpectedState, record.CurrentState)
	}
	compiled, err := graph.AdmitTransition(*site, record.ExpectedState, record.CurrentState)
	if err != nil {
		t.Fatal(err)
	}
	if record.ExpectedState == record.CurrentState {
		// Repeating a loop changes its generation, not its stage-entry identity.
		return nil
	}
	transition, err := workflowlifecycle.NewCompiledTransition(compiled, handlerselection.NotApplicable(), nil)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := workflowlifecycle.NewAcceptedEvent(record.Identity.Route, identity.NormalizeEntityID(record.EntityID), event.ID(), string(event.Type()), event.ExecutionMode(), event.CreatedAt(), &transition)
	if err != nil {
		t.Fatal(err)
	}
	effect, err = effect.WithExecutionOccurrence("delivery", deliveryID)
	if err != nil {
		t.Fatal(err)
	}
	entry, enters, err := effect.StageEntry(record.Identity)
	if err != nil || !enters {
		t.Fatalf("fan-out admitted stage entry: %t %v", enters, err)
	}
	bookkeeping := map[string]any{}
	if err := json.Unmarshal(record.Bookkeeping, &bookkeeping); err != nil {
		t.Fatal(err)
	}
	if err := workflowlifecycle.StoreStageEntry(bookkeeping, entry); err != nil {
		t.Fatal(err)
	}
	record.Bookkeeping, err = json.Marshal(bookkeeping)
	if err != nil {
		t.Fatal(err)
	}
	return &entry
}
