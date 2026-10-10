package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type failOnceRetryPipelineBus struct {
	*nativePipelineDeliveryBusObservationForTest
	calls atomic.Int32
}

type failingPreclaimDeliveryStore struct {
	runtimedelivery.Store
	cancel  context.CancelFunc
	failure error
	called  bool
}

func (s *failingPreclaimDeliveryStore) ClaimDelivery(context.Context, runtimedelivery.ExecutionAuthority, events.Event, events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	s.called = true
	if s.cancel != nil {
		s.cancel()
	}
	return runtimedelivery.ClaimResult{}, s.failure
}

func VerifyPipelinePreclaimFailurePreservesErrorAndReturnsExactCarrierForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	independent := errors.New("independent claim-store failure")
	for _, tc := range []struct {
		name         string
		failure      error
		cancel       bool
		wantCanceled bool
		wantStore    bool
	}{
		{name: "cancellation", failure: context.Canceled, cancel: true, wantCanceled: true},
		{name: "store_failure", failure: independent, wantStore: true},
		{name: "joined_failure", failure: errors.Join(context.Canceled, independent), cancel: true, wantCanceled: true, wantStore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := semanticview.Wrap(deliveryAuthoritySourceForTest(t))
			fixture := open(t, source)
			bundle, _ := semanticview.Bundle(source)
			module := handlerTestWorkflowModuleWithBundle(bundle, ".", "node-a").(*previewWorkflowModule)
			node := pipelineNode(t, ".", "node-a")
			module.workflowNodes = []WorkflowNode{{Node: node, Subscriptions: []events.EventType{"source.evt"}}}
			pc := fixture.NewCoordinator(PipelineCoordinatorOptions{Module: module})
			runCtx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
			if err := fixture.RequireRun(runCtx, testPipelineRunID); err != nil {
				t.Fatal(err)
			}
			entityID := testPipelineRunID
			if err := fixture.Construct(runCtx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: testPipelineRunID, StorageRef: testPipelineRunID, EntityID: entityID,
				WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), CurrentState: "queued", Fields: map[string]any{}, EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}
			target := events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: entityID}
			evt := eventtest.ExistingRunRootIngress(uuid.NewString(), "source.evt", "src", "", []byte(`{"entity_id":"`+entityID+`"}`), 0,
				testPipelineRunID, events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Now().UTC())
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target)}
			fixture.Publish(runCtx, evt, route)
			delivery, err := events.NewDeliveryEvent(evt, route)
			if err != nil {
				t.Fatal(err)
			}
			deliveryID, err := runtimedelivery.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			pending, err := pc.deliveryStore.Snapshot(runCtx, deliveryID)
			if err != nil || pending.EventID != evt.ID() || pending.Route.Target != route.Target || pending.Route.Recipient != route.Recipient || pending.Status != runtimedelivery.StatusPending {
				t.Fatalf("preclaim fixture differs from exact pending delivery: %+v err=%v", pending, err)
			}
			readAttempts := func() []string {
				raw, err := fixture.ApplicationStorage(runCtx)
				if err != nil {
					t.Fatal(err)
				}
				var snapshot map[string]struct{ Rows []string }
				if err := json.Unmarshal(raw, &snapshot); err != nil {
					t.Fatal(err)
				}
				value, found := snapshot["event_delivery_attempts"]
				if !found {
					t.Fatal("preclaim snapshot omitted physical delivery attempts")
				}
				return value.Rows
			}
			if attempts := readAttempts(); len(attempts) != 0 {
				t.Fatalf("preclaim fixture already has attempts: %q", attempts)
			}
			continuation := &scriptedWorkflowNodeContinuation{deliveryID: deliveryID}
			ctx, cancel := context.WithCancel(runCtx)
			defer cancel()
			guard, err := worklifetime.NewDeliveryContinuationGuard(ctx, continuation)
			if err != nil {
				t.Fatal(err)
			}
			ctx, err = worklifetime.WithDirectDeliveryCarrier(ctx, guard)
			if err != nil {
				t.Fatal(err)
			}
			store := &failingPreclaimDeliveryStore{Store: pc.deliveryStore, failure: tc.failure}
			if tc.cancel {
				store.cancel = cancel
			}
			pc.deliveryStore = store
			_, _, outcome, err := pc.InterceptDeliveryRoute(ctx, delivery, route)
			if !store.called || err == nil || errors.Is(err, context.Canceled) != tc.wantCanceled || errors.Is(err, independent) != tc.wantStore {
				t.Fatalf("claim called=%t error=%v; want cancellation=%t store=%t", store.called, err, tc.wantCanceled, tc.wantStore)
			}
			if _, settled := outcome.Disposition(); settled || !outcome.ContinueDispatch() {
				t.Fatalf("preclaim failure requested event settlement: %+v", outcome)
			}
			if resolution, ok := guard.Resolution(); !ok || resolution != worklifetime.DeliveryContinuationReturned || continuation.returns.Load() != 1 || continuation.consumes.Load() != 0 {
				t.Fatalf("carrier resolution=%v present=%t returns=%d consumes=%d; want exact return", resolution, ok, continuation.returns.Load(), continuation.consumes.Load())
			}
			if attempts := readAttempts(); len(attempts) != 0 {
				t.Fatalf("delivery authority node outcomes = %d, want 0", len(attempts))
			}
			after, err := pc.deliveryStore.Snapshot(runCtx, deliveryID)
			status := string(after.Status)
			if err != nil || status != "pending" {
				t.Fatalf("preclaim delivery status=%q error=%v, want pending", status, err)
			}
			if !reflect.DeepEqual(pending, after) {
				t.Fatalf("preclaim changed exact delivery snapshot: before=%+v after=%+v", pending, after)
			}
		})
	}
}

func (b *failOnceRetryPipelineBus) PrepareEnginePublications(ctx context.Context, intents []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error) {
	if b.calls.Add(1) == 1 {
		return nil, runtimefailures.Wrap(
			runtimefailures.ClassDependencyUnavailable,
			"prepare_failed",
			"workflow-node-retry-test",
			"prepare_engine_publications",
			nil,
			errors.New("transient publication preparation failure"),
		)
	}
	return b.EngineMutationPublicationPlanner.PrepareEnginePublications(ctx, intents)
}

func (b *failOnceRetryPipelineBus) PrepareEngineMutationPublications(ctx context.Context, intents []runtimeengine.EmitIntent, prospective PreparedWorkflowPublicationState) ([]runtimeengine.DurablePublicationPlan, error) {
	if prospective.Empty() {
		return nil, errors.New("test mutation publication requires prepared state")
	}
	if b.calls.Add(1) == 1 {
		return nil, runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "prepare_failed", "workflow-node-retry-test", "prepare_engine_publications", nil, errors.New("transient publication preparation failure"))
	}
	return b.EngineMutationPublicationPlanner.PrepareEngineMutationPublications(ctx, intents, prospective)
}

func VerifyPipelineCoordinatorInterceptSkipsNodeWithoutPersistedDeliveryAuthorityForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := semanticview.Wrap(deliveryAuthoritySourceForTest(t))
	fixture := open(t, source)
	bundle, _ := semanticview.Bundle(source)
	module := handlerTestWorkflowModuleWithBundle(bundle, ".", "node-a").(*previewWorkflowModule)
	node := pipelineNode(t, ".", "node-a")
	module.workflowNodes = []WorkflowNode{{Node: node, Subscriptions: []events.EventType{"source.evt"}}}
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{Module: module})
	runCtx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
	if err := fixture.RequireRun(runCtx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}
	entityID := testPipelineRunID
	if err := fixture.Construct(runCtx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: testPipelineRunID, StorageRef: testPipelineRunID, EntityID: entityID,
		WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), CurrentState: "queued", Fields: map[string]any{}, EntityType: "test_entity",
	})); err != nil {
		t.Fatal(err)
	}
	target := events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: entityID}
	evt := eventtest.ExistingRunRootIngress(uuid.NewString(), "source.evt", "src", "", []byte(`{"entity_id":"`+entityID+`"}`), 0,
		testPipelineRunID, events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Now().UTC())
	fixture.PublishDirect(runCtx, evt)
	readCounts := func() map[string]int {
		raw, err := fixture.ApplicationStorage(runCtx)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot map[string]struct{ Rows []string }
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		counts := map[string]int{}
		for _, table := range []string{"events", "event_deliveries", "event_delivery_attempts"} {
			value, found := snapshot[table]
			if !found {
				t.Fatalf("unstamped fixture omitted %s", table)
			}
			counts[table] = len(value.Rows)
		}
		return counts
	}
	before := readCounts()
	if before["events"] != 1 || before["event_deliveries"] != 0 || before["event_delivery_attempts"] != 0 {
		t.Fatalf("unstamped precondition = %+v, want one event without delivery or attempt", before)
	}

	ictx := runCtx
	passthrough, _, _, err := pc.Intercept(ictx, evt)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passthrough {
		t.Fatal("Intercept passthrough = false, want event-wide interception to leave unstamped node delivery untouched")
	}
	after := readCounts()
	if got := after["events"] - before["events"]; got != 0 {
		t.Fatalf("published events = %d, want 0 without node delivery authority", got)
	}
	if after["event_delivery_attempts"] != 0 || after["event_deliveries"] != 0 {
		t.Fatalf("delivery authority node outcomes/deliveries = %+v, want zero", after)
	}
}

func VerifyNativePipelineCoordinatorInterceptDeliveryRouteConsumesTargetWithoutGenericAuthorityLogForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx, evt, route := nativeReceiverPreparationFixtureForTest(t, backend, open)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			delivery, err := events.NewDeliveryEvent(evt, route)
			if err != nil {
				t.Fatal(err)
			}
			passthrough, _, _, err := pc.InterceptDeliveryRoute(ctx, delivery, route)
			if err != nil {
				t.Fatalf("target InterceptDeliveryRoute: %v", err)
			}
			if passthrough {
				t.Fatal("target InterceptDeliveryRoute passthrough = true, want consumed target-routed event")
			}
			if deliveryAuthorityLogCount(bus.runtimeLogEntries()) != 0 {
				t.Fatalf("target runtime logs = %#v, want no false delivery_authority_missing log", bus.runtimeLogEntries())
			}
			id, err := runtimedelivery.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			outcomes, err := fixture.Store.Outcomes(ctx, id)
			if err != nil || len(outcomes) != 1 {
				t.Fatalf("exact target outcomes = %#v/%v, want one", outcomes, err)
			}
		})
	}
}

func VerifyNativePipelineCoordinatorInterceptDeliveryRouteRejectsConnectedInputReplayWithoutStampedClaimForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	source := loadNamesOnlyPipelineSource(t, canonicalrouting.CopyPipelineConnectedDeliveryCollision(t))
	bundle, _ := semanticview.Bundle(source)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := open(t, backend, semanticview.Wrap(bundle))
			node := pipelineNode(t, "receiver", "receiver-node")
			module := &previewWorkflowModule{bundle: bundle, workflowNodes: []WorkflowNode{{Node: node, Subscriptions: []events.EventType{"deploy.accepted", "deploy.audited"}}}}
			pc, ctx := nativePipelineDeliveryCoordinatorForTest(t, fixture, module)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			run := uuid.NewString()
			ctx = runtimecorrelation.WithRunID(ctx, run)
			if err := fixture.RequireRun(ctx, run); err != nil {
				t.Fatal(err)
			}
			target := events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver", EntityID: uuid.NewString()}
			producerEntity := uuid.NewString()
			evt := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "producer/deploy.done", "producer", "", []byte("{}"), 0, run,
				events.EventEnvelope{EntityID: target.EntityID, FlowInstance: target.FlowInstance,
					Source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: producerEntity}, Target: target},
				eventtest.StaticFlowRoutingSource("producer", "producer", producerEntity), time.Now().UTC())
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target)}
			if err := fixture.PublishNode(ctx, evt, route); err != nil {
				t.Fatal(err)
			}
			delivery, err := events.NewDeliveryEvent(evt, route)
			if err != nil {
				t.Fatal(err)
			}
			for attempt := 1; attempt <= 2; attempt++ {
				passthrough, deferred, _, err := pc.InterceptDeliveryRoute(ctx, delivery, route)
				if err == nil || !strings.Contains(err.Error(), "stamped connect claim") {
					t.Fatalf("attempt %d InterceptDeliveryRoute error = %v, want missing stamped-claim failure", attempt, err)
				}
				if passthrough {
					t.Fatalf("attempt %d passthrough = true, want fail-closed interception", attempt)
				}
				if len(deferred) != 0 {
					t.Fatalf("attempt %d deferred events = %#v, want none", attempt, deferred)
				}
			}
			id, err := runtimedelivery.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			outcomes, err := fixture.Store.Outcomes(ctx, id)
			if err != nil || len(outcomes) != 0 {
				t.Fatalf("unstamped replay outcomes = %#v/%v, want none", outcomes, err)
			}
			snapshot, err := fixture.Store.Snapshot(ctx, id)
			if err != nil || snapshot.Status != runtimedelivery.StatusPending {
				t.Fatalf("unstamped replay changed exact pending obligation: %+v/%v", snapshot, err)
			}
			if got := bus.publishedCount(); got != 0 {
				t.Fatalf("published handler events = %d, want zero", got)
			}
		})
	}
}

func VerifyNativePipelineCoordinatorInterceptTerminalNodeDeliveryDoesNotAuthorizeExecutionForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend+"/dead_letter", func(t *testing.T) {
			fixture, pc, ctx, evt, route := nativeReceiverPreparationFixtureForTest(t, backend, open)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			id, err := runtimedelivery.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			consumeNativePipelineDeliveryCarrierForTest(t, fixture, ctx, id)
			result, err := fixture.Store.ClaimDelivery(ctx, fixture.Authority, evt, route)
			if err != nil {
				t.Fatal(err)
			}
			acquired, ok := result.Acquired()
			if !ok {
				t.Fatalf("terminal fixture claim = %s, want acquired", result.Disposition)
			}
			failure := runtimefailures.FromError(errors.New("terminal delivery fixture"), "pipeline-test", "settle").Failure
			if _, err := fixture.Store.SettleFailure(ctx, acquired.Claim, runtimedelivery.Settlement{
				Disposition: runtimedelivery.FailureDeadLetter, ReasonCode: "terminal_delivery_fixture",
				Failure: &failure, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation(),
			}); err != nil {
				t.Fatal(err)
			}
			passthrough, _, _, err := pc.Intercept(ctx, evt)
			if err != nil {
				t.Fatalf("Intercept: %v", err)
			}
			if !passthrough {
				t.Fatal("event-wide interception must leave terminal node delivery untouched")
			}
			if got := bus.publishedCount(); got != 0 {
				t.Fatalf("terminal delivery emitted %d events", got)
			}
			outcomes, err := fixture.Store.Outcomes(ctx, id)
			if err != nil || len(outcomes) != 1 {
				t.Fatalf("terminal outcomes = %#v/%v, want one", outcomes, err)
			}
			if count, err := fixture.DeliveryCount(ctx, evt.ID(), route.Recipient.ID()); err != nil || count != 1 {
				t.Fatalf("terminal exact physical obligations = %d/%v, want one", count, err)
			}
		})
	}
}

func VerifyNativePipelineCoordinatorInterceptSettlesAuthorizedNodeDeliveryForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx, evt, route := nativeReceiverPreparationFixtureForTest(t, backend, open)
			handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
			if err != nil || !handled {
				t.Fatalf("authorized node execution handled=%t error=%v", handled, err)
			}
			id, err := runtimedelivery.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			outcomes, err := fixture.Store.Outcomes(ctx, id)
			if err != nil || len(outcomes) != 1 {
				t.Fatalf("authorized outcomes = %#v/%v, want one", outcomes, err)
			}
			snapshot, err := fixture.Store.Snapshot(ctx, id)
			if err != nil || snapshot.Status != runtimedelivery.StatusDelivered {
				t.Fatalf("authorized exact snapshot = %+v/%v, want delivered", snapshot, err)
			}
		})
	}
}

func VerifyNativeWorkflowNodeRetryWaitSurvivesHeartbeatSettlementParityForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	const retryBase = 30 * time.Second
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			source := loadNamesOnlyPipelineSource(t, canonicalrouting.CopyPipelineDeliveryRetry(t))
			bundle, _ := semanticview.Bundle(source)
			module := handlerTestWorkflowModuleWithBundle(bundle, ".", "node-a").(*previewWorkflowModule)
			module.workflowNodes = []WorkflowNode{{
				Node: pipelineNode(t, ".", "node-a"), Subscriptions: []events.EventType{"source.evt"},
			}}
			fixture := open(t, backend, semanticview.Wrap(bundle))
			pc, ctx := nativePipelineDeliveryCoordinatorForTest(t, fixture, module)
			owner := fixture.Store
			bus := &failOnceRetryPipelineBus{nativePipelineDeliveryBusObservationForTest: observeNativePipelineDeliveryBusForTest(t, pc)}
			pc.bus = bus
			runID := uuid.NewString()
			ctx = runtimecorrelation.WithRunID(ctx, runID)
			if err := fixture.RequireRun(ctx, runID); err != nil {
				t.Fatal(err)
			}
			entityID := runID
			evt := eventtest.ExistingRunRootIngress(uuid.NewString(), "source.evt", "src", "", []byte("{}"), 0, runID,
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID}), time.Now().UTC())
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: runID, StorageRef: runID, WorkflowName: ".", WorkflowVersion: pc.SemanticSource().WorkflowVersion(), CurrentState: "queued",
				EntityID: entityID, EnteredStageAt: evt.CreatedAt(), CreatedAt: evt.CreatedAt(), EntityType: "test_entity", Fields: map[string]any{},
			})); err != nil {
				t.Fatal(err)
			}

			route := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(pipelineNode(t, ".", "node-a")), Target: events.MustExistingEntityTarget(events.RouteIdentity{
					FlowID: ".", FlowInstance: runID, EntityID: entityID,
				}),
			}
			if err := fixture.PublishNode(ctx, evt, route); err != nil {
				t.Fatalf("commit node delivery: %v", err)
			}

			handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
			if err != nil {
				t.Fatalf("dispatch retrying node delivery: %v", err)
			}
			if !handled {
				t.Fatal("dispatch retrying node delivery handled = false, want true")
			}
			if got := bus.calls.Load(); got == 0 {
				t.Fatal("publication preparation failure injector was not reached")
			}
			proof, err := owner.ProveHandoff(ctx, evt.ID(), route)
			if err != nil {
				t.Fatalf("prove node delivery handoff: %v", err)
			}
			snapshot, err := owner.Snapshot(ctx, proof.DeliveryID())
			if err != nil {
				t.Fatalf("load node delivery snapshot: %v", err)
			}
			outcomes, err := owner.Outcomes(ctx, proof.DeliveryID())
			if err != nil {
				t.Fatalf("load node delivery outcomes: %v", err)
			}
			if snapshot.Status != runtimedelivery.StatusFailed || snapshot.RetryCount != 1 {
				t.Fatalf(
					"node delivery snapshot = status:%s retries:%d next:%s local-now:%s outcomes:%#v, want failed/1 after exact continuation transfer",
					snapshot.Status,
					snapshot.RetryCount,
					snapshot.NextEligibleAt,
					time.Now().UTC(),
					outcomes,
				)
			}
			if got := snapshot.NextEligibleAt.Sub(snapshot.UpdatedAt); got != retryBase {
				t.Fatalf("node delivery retry cadence = %s, want %s", got, retryBase)
			}
			if len(outcomes) != 1 || outcomes[0].Outcome != "retry_scheduled" {
				t.Fatalf("node delivery outcomes = %#v, want retry_scheduled after first attempt", outcomes)
			}

			handled, err = pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
			if err != nil {
				t.Fatalf("dispatch early retry wake: %v", err)
			}
			if !handled {
				t.Fatal("dispatch early retry wake handled = false, want exact deferred disposition")
			}
			snapshot, err = owner.Snapshot(ctx, proof.DeliveryID())
			if err != nil {
				t.Fatalf("load early-wake delivery snapshot: %v", err)
			}
			if snapshot.Status != runtimedelivery.StatusFailed || snapshot.RetryCount != 1 {
				t.Fatalf("early-wake delivery snapshot = status:%s retries:%d, want unchanged failed/1", snapshot.Status, snapshot.RetryCount)
			}

			if err := fixture.RetryEligible(ctx, evt, route); err != nil {
				t.Fatalf("make retry selected-store eligible: %v", err)
			}
			recoverNativePipelineRetryForTest(t, fixture, ctx)
			handled, err = pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
			if err != nil {
				t.Fatalf("dispatch selected-store-eligible retry: %v", err)
			}
			if !handled {
				t.Fatal("dispatch selected-store-eligible retry handled = false, want delivered")
			}
			snapshot, err = owner.Snapshot(ctx, proof.DeliveryID())
			if err != nil {
				t.Fatalf("load delivered retry snapshot: %v", err)
			}
			outcomes, err = owner.Outcomes(ctx, proof.DeliveryID())
			if err != nil {
				t.Fatalf("load delivered retry outcomes: %v", err)
			}
			if snapshot.Status != runtimedelivery.StatusDelivered || snapshot.RetryCount != 1 {
				t.Fatalf("delivered retry snapshot = status:%s retries:%d, want delivered/1", snapshot.Status, snapshot.RetryCount)
			}
			if len(outcomes) != 2 || outcomes[0].Outcome != "retry_scheduled" || outcomes[1].Outcome != string(runtimedelivery.StatusDelivered) {
				t.Fatalf("delivered retry outcomes = %#v, want retry_scheduled then delivered", outcomes)
			}
		})
	}
}

func deliveryAuthoritySourceForTest(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	source := loadNamesOnlyPipelineSource(t, canonicalrouting.CopyPipelineDeliveryAuthority(t))
	bundle, _ := semanticview.Bundle(source)
	return bundle
}

func deliveryAuthorityLogCount(logs []RuntimeLogEntry) int {
	count := 0
	for _, log := range logs {
		if log.Action == "delivery_authority_missing" {
			count++
		}
	}
	return count
}
