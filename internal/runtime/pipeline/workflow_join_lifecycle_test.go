package pipeline

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/uuid"
)

type workflowJoinLifecycleSemanticSource struct {
	semanticview.Source
}

func (s workflowJoinLifecycleSemanticSource) ExecutableNodeSource(node identity.ExecutableNode) (runtimecontracts.ContractItemSource, bool) {
	switch node.NodeID() {
	case "join-node", "dispatcher":
		return runtimecontracts.ContractItemSource{FlowPath: node.FlowPath(), Family: "nodes"}, true
	default:
		return s.Source.ExecutableNodeSource(node)
	}
}

func workflowJoinLifecycleSource(bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	return workflowJoinLifecycleSemanticSource{Source: semanticview.Wrap(bundle)}
}

func VerifyNativeWorkflowLifecycleOwnerIsConstructedBeforeDurableStoreReachabilityOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			_, coordinator, _, _ := nativeWorkflowJoinCoordinatorForTest(t, backend, workflowJoinLifecycleBundle(t), nil, open)
			if coordinator == nil || coordinator.workflowStore == nil || coordinator.workflowStore.lifecycleOwner == nil {
				t.Fatal("durable workflow store became reachable without its canonical lifecycle owner")
			}
		})
	}
}

func VerifyNativeWorkflowJoinUsesSelectedStoreScheduleOwnerOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := workflowJoinLifecycleBundle(t)
			schedules := &recordingGenericScheduleWakeupOwner{}
			fixture, pc, ctx, mutations := nativeWorkflowJoinCoordinatorForTest(t, backend, bundle, schedules, open)
			store := pc.workflowStore
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
				InstanceID: strings.TrimPrefix(path, "orders/"), StorageRef: path, WorkflowName: "orders", WorkflowVersion: bundle.WorkflowVersion(),
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"expected": []any{"a"}, "instance_key": strings.TrimPrefix(path, "orders/")},
				EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}

			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute(path), entityID); err != nil {
				t.Fatalf("arm selected-store join: %v", err)
			}
			instance, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !ok {
				t.Fatalf("load after rejected ownerless join = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			if activation, found, loadErr := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey(t, carrier.StateBuckets, mustPipelineNode("orders", "join-node"))); loadErr != nil || !found {
				t.Fatalf("selected-store join activation = %#v, found=%v error=%v", activation, found, loadErr)
			}
			upserts, _ := mutations.schedules()
			if len(upserts) != 1 || upserts[0].Command.EventType != joinTimeoutEvent {
				t.Fatalf("selected-store join schedules = %#v, want one timeout", upserts)
			}
			if len(schedules.activationIDs) != 1 || schedules.activationIDs[0] != upserts[0].ID {
				t.Fatalf("selected-store join wakeup reconciliation = %#v, want activation %s", schedules.activationIDs, upserts[0].ID)
			}
		})
	}
}

func VerifyNativeWorkflowJoinSchedulePreservesMockExecutionModeOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := workflowJoinLifecycleBundle(t)
			schedules := &recordingGenericScheduleWakeupOwner{}
			fixture, pc, ctx, mutations := nativeWorkflowJoinCoordinatorForTest(t, backend, bundle, schedules, open)
			ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Mock)
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			enteredAt := time.Now().UTC()
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
				InstanceID: strings.TrimPrefix(path, "orders/"), StorageRef: path, WorkflowName: "orders", WorkflowVersion: bundle.WorkflowVersion(),
				CurrentState: "dispatching", EnteredStageAt: enteredAt, Fields: map[string]any{"expected": []any{"a"}, "instance_key": strings.TrimPrefix(path, "orders/")},
				EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}
			inbound := nativeWorkflowJoinEventForTest(ctx, "orders", path, entityID, "orders/order.accepted", []byte(`{"line_items":[]}`), enteredAt)
			dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, inbound, "dispatcher")
			upserts, _ := mutations.schedules()
			if len(upserts) != 1 || upserts[0].Command.ExecutionMode != executionmode.Mock {
				t.Fatalf("mock join schedules = %#v, want one mock timeout", upserts)
			}
		})
	}
}

func VerifyNativeArmWorkflowJoinPersistsActivationAndScheduleAtomicallyForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range []struct {
		name       string
		members    []any
		wantStatus joinruntime.Status
		wantReason joinruntime.CloseReason
		wantEvent  string
		wantKind   timeridentity.TimerHandleKind
	}{
		{name: "members wait on timeout", members: []any{"a", "b"}, wantStatus: joinruntime.StatusOpen, wantEvent: joinTimeoutEvent, wantKind: timeridentity.TimerHandleJoinTimeout},
		{name: "zero members complete immediately", members: []any{}, wantStatus: joinruntime.StatusClosed, wantReason: joinruntime.CloseReasonComplete, wantEvent: joinCompleteEvent, wantKind: timeridentity.TimerHandleJoinComplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schedules := &recordingGenericScheduleWakeupOwner{}
			bundle := workflowJoinLifecycleBundle(t)
			fixture, pc, ctx, mutations := nativeWorkflowJoinCoordinatorForTest(t, "sqlite", bundle, schedules, open)
			store := pc.workflowStore
			entityID := FlowInstanceEntityID("orders/order-1")
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
				InstanceID: "order-1", StorageRef: "orders/order-1", WorkflowName: "orders", WorkflowVersion: "1.0.0",
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"expected": tc.members, "instance_key": "order-1"},
				EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}
			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute("orders/order-1"), entityID); err != nil {
				t.Fatalf("arm join: %v", err)
			}
			instance, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, "orders/order-1"))
			if err != nil || !ok {
				t.Fatalf("load instance = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey(t, carrier.StateBuckets, mustPipelineNode("orders", "join-node")))
			if err != nil || !ok {
				t.Fatalf("load activation = %#v, %v, %v", activation, ok, err)
			}
			if activation.Status != tc.wantStatus || activation.CloseReason != tc.wantReason {
				t.Fatalf("activation status = %s/%s, want %s/%s", activation.Status, activation.CloseReason, tc.wantStatus, tc.wantReason)
			}
			upserts, _ := mutations.schedules()
			if got := len(upserts); got != 1 {
				t.Fatalf("schedules = %d, want 1", got)
			}
			schedule := upserts[0]
			if schedule.Command.EventType != tc.wantEvent || schedule.Command.EntityID != entityID {
				t.Fatalf("schedule = %#v", schedule)
			}
			handle, ok := timeridentity.ParseTimerHandle(parsePayloadMap(genericSchedulePayloadForTest(t, schedule)))
			ref, refOK := handle.JoinRef()
			if !ok || !refOK || handle.Kind() != tc.wantKind || ref.JoinID() != "awaiting" {
				t.Fatalf("timer handle = %#v, %v", handle, ok)
			}
		})
	}
}

func VerifyNativeArmWorkflowJoinPostgresParityForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range []struct {
		name       string
		members    []any
		wantStatus joinruntime.Status
		wantEvent  string
	}{
		{name: "members", members: []any{"a"}, wantStatus: joinruntime.StatusOpen, wantEvent: joinTimeoutEvent},
		{name: "expected zero", members: []any{}, wantStatus: joinruntime.StatusClosed, wantEvent: joinCompleteEvent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schedules := &recordingGenericScheduleWakeupOwner{}
			fixture, pc, ctx, mutations := nativeWorkflowJoinCoordinatorForTest(t, "postgres", workflowJoinLifecycleBundle(t), schedules, open)
			store := pc.workflowStore
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{InstanceID: strings.TrimPrefix(path, "orders/"), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"expected": tc.members, "instance_key": strings.TrimPrefix(path, "orders/")},
				EntityType: "test_entity"})); err != nil {
				t.Fatal(err)
			}
			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute(path), entityID); err != nil {
				t.Fatal(err)
			}
			instance, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !ok {
				t.Fatalf("load = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey(t, carrier.StateBuckets, mustPipelineNode("orders", "join-node")))
			if err != nil || !ok || activation.Status != tc.wantStatus {
				t.Fatalf("activation = %#v, %v, %v", activation, ok, err)
			}
			upserts, _ := mutations.schedules()
			if len(upserts) != 1 || upserts[0].Command.EventType != tc.wantEvent {
				t.Fatalf("schedule parity = schedules:%#v", upserts)
			}
		})
	}
}

func VerifyNativeWorkflowJoinCountWaitsDespiteEmptyStateMembersOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			files := workflowJoinLifecycleFixtureFiles(false, "")
			files["orders/nodes.yaml"] = strings.Replace(files["orders/nodes.yaml"], "members: {from: state.expected, by: payload.member_id}", "members: {count: 1, by: payload.member_id}", 1)
			bundle := loadWorkflowTempBundle(t, files)
			schedules := &recordingGenericScheduleWakeupOwner{}
			fixture, pc, ctx, mutations := nativeWorkflowJoinCoordinatorForTest(t, backend, bundle, schedules, open)
			store := pc.workflowStore
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
				InstanceID: strings.TrimPrefix(path, "orders/"), StorageRef: path, WorkflowName: "orders", WorkflowVersion: bundle.WorkflowVersion(),
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"expected": []any{}, "instance_key": strings.TrimPrefix(path, "orders/")},
				EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}
			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute(path), entityID); err != nil {
				t.Fatalf("arm custom join: %v", err)
			}
			instance, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !ok {
				t.Fatalf("load custom join = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey(t, carrier.StateBuckets, mustPipelineNode("orders", "join-node")))
			if err != nil || !ok || activation.Status != joinruntime.StatusOpen || activation.CloseReason != "" {
				t.Fatalf("count activation = %#v, %v, %v, want open", activation, ok, err)
			}
			upserts, _ := mutations.schedules()
			if len(upserts) != 1 || upserts[0].Command.EventType != joinTimeoutEvent {
				t.Fatalf("count schedules = %#v, want deadline", upserts)
			}
		})
	}
}

func TestWorkflowJoinOutcomeRejectsCatalogInvalidNamedResultExpression(t *testing.T) {
	resultType := runtimecontracts.CatalogTypeReference{
		Type: "JoinResult",
		Catalog: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"JoinResult": {Fields: map[string]runtimecontracts.TypeFieldSpec{"value": {Type: "text"}}},
		}},
	}

	options := workflowexpr.ValueExpressionOptions{AllowJoin: true, JoinResultType: resultType, JoinContext: workflowexpr.JoinContextArrival}
	if err := workflowexpr.ValidateValueExpressionWithOptions(`join.results.exists(r, r.value == "ok")`, options); err != nil {
		t.Fatal(err)
	}
	err := workflowexpr.ValidateValueExpressionWithOptions("join.results.exists(r, r > 1)", options)
	if err == nil || !strings.Contains(err.Error(), "no matching overload") {
		t.Fatalf("join outcome error = %v, want catalog-backed typed rejection", err)
	}
}

func VerifyNativeWorkflowJoinDurableIdentityIncludesStageOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := workflowJoinLifecycleBundleWithReview(t, true)
			schedules := &recordingGenericScheduleWakeupOwner{}
			fixture, pc, ctx, mutations := nativeWorkflowJoinCoordinatorForTest(t, backend, bundle, schedules, open)
			store := pc.workflowStore
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
				InstanceID: strings.TrimPrefix(path, "orders/"), StorageRef: path, WorkflowName: "orders", WorkflowVersion: bundle.WorkflowVersion(),
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"expected": []any{"a"}, "instance_key": strings.TrimPrefix(path, "orders/")},
				EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}
			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute(path), entityID); err != nil {
				t.Fatal(err)
			}
			transitionAt := time.Now().UTC()
			inbound := nativeWorkflowJoinEventForTest(ctx, "orders", path, entityID, "orders/review.requested", []byte(`{}`), transitionAt)
			upserts, _ := mutations.schedules()
			_, awaitingRef, awaitingFound := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, upserts[0])))
			if !awaitingFound {
				t.Fatal("awaiting schedule has no retained arm")
			}
			dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, inbound, "dispatcher")
			instance, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !ok {
				t.Fatalf("load stage-scoped joins = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			awaiting, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), joinruntime.ActivationKey(awaitingRef))
			if err != nil || !ok || awaiting.CloseReason != joinruntime.CloseReasonStageExit {
				t.Fatalf("awaiting activation = %#v, %v, %v", awaiting, ok, err)
			}
			upserts, _ = mutations.schedules()
			_, reviewingRef, reviewingFound := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, upserts[1])))
			if !reviewingFound {
				t.Fatal("reviewing schedule has no retained arm")
			}
			reviewing, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), joinruntime.ActivationKey(reviewingRef))
			if err != nil || !ok || reviewing.Status != joinruntime.StatusOpen || reviewing.Stage() != "reviewing" {
				t.Fatalf("reviewing activation = %#v, %v, %v", reviewing, ok, err)
			}
			if awaiting.Key() == reviewing.Key() || len(upserts) != 2 {
				t.Fatalf("stage identities/schedules = awaiting:%q reviewing:%q schedules:%#v", awaiting.Key(), reviewing.Key(), upserts)
			}
		})
	}
}

type workflowJoinStoreCase struct {
	name string
}

func genericSchedulePayloadForTest(t *testing.T, activation runtimegenericschedule.Activation) json.RawMessage {
	t.Helper()
	payload, err := canonicaljson.Encode(activation.Command.Payload)
	if err != nil {
		t.Fatalf("encode generic schedule payload: %v", err)
	}
	return payload
}

func workflowJoinScheduleEventForTest(t *testing.T, id string, activation runtimegenericschedule.Activation, runID string, envelope events.EventEnvelope, at time.Time) events.Event {
	t.Helper()
	return eventtest.RuntimeControlWithRoutingSource(
		eventtest.UUID(id),
		events.EventType(activation.Command.EventType),
		runtimegenericschedule.OccurrenceProducerID(),
		activation.Command.TaskID,
		genericSchedulePayloadForTest(t, activation),
		0,
		runID,
		"",
		envelope,
		activation.Command.RoutingSource,
		at,
	)
}

func workflowJoinStoreCases() []workflowJoinStoreCase {
	return []workflowJoinStoreCase{{name: "sqlite"}, {name: "postgres"}}
}

func VerifyNativeWorkflowJoinArrivalTimeoutRaceHasOneCloseWinnerOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			h := newNativeExactWorkflowJoinHarness(t, tc.name, "orders", "awaiting", []any{"a"}, nil, open)
			fixture, pc, ctx, store := h.fixture, h.pc, h.ctx, h.store
			path, entityID := h.path, h.entityID
			schedule := h.armInitial()
			activation := h.activation()
			ref := activation.JoinRef()
			handle, err := timeridentity.JoinTimeoutHandle(ref)
			if err != nil {
				t.Fatal(err)
			}
			if schedule.Command.TaskID != handle.TaskID() {
				t.Fatal("race does not use its exact native scheduled arm")
			}
			now := activation.ArmedAt

			handler := pc.SemanticSource().ExecutableNodeEventHandlers(mustPipelineNode("orders", "join-node"))["item.completed"]
			member := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("member-a"), events.EventType("item.completed"), "", "", json.RawMessage(`{"member_id":"a","result":{"ok":true}}`), 0, runtimecorrelation.RunIDFromContext(ctx), h.envelope(), testWorkflowRoutingSource("orders", path, entityID), now)
			timeout := workflowJoinScheduleEventForTest(t, "timeout-a", schedule, runtimecorrelation.RunIDFromContext(ctx), h.envelope(), now.Add(time.Hour))
			delivery := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPipelineNode("orders", "join-node")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "orders", FlowInstance: path, EntityID: entityID})}
			memberCtx, err := nativeWorkflowJoinPublicationContextForTest(t, fixture, pc, ctx, member, delivery, true)
			if err != nil {
				t.Fatal(err)
			}
			timerCtx, err := nativeWorkflowJoinPublicationContextForTest(t, fixture, pc, ctx, timeout, delivery, false)
			if err != nil {
				t.Fatal(err)
			}
			triggerState := mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(path), entityID)
			type raceResult struct {
				result contractHandlerExecutionResult
				err    error
			}
			start := make(chan struct{})
			results := make(chan raceResult, 2)
			var wg sync.WaitGroup
			for _, work := range []struct {
				event events.Event
				ctx   context.Context
			}{{member, memberCtx}, {timeout, timerCtx}} {
				work := work
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					result, err := executeNativeClaimedPipelineHandlerForTest(t, pc, work.ctx, mustPipelineNode("orders", "join-node"), handler, workflowTriggerContext{Event: work.event, State: triggerState, HandlerEventKey: "item.completed"})
					results <- raceResult{result: result, err: err}
				}()
			}
			close(start)
			wg.Wait()
			close(results)
			for result := range results {
				if result.err != nil {
					envelope, ok := runtimefailures.EnvelopeFromError(result.err)
					if !ok || envelope.Class != runtimefailures.ClassStaleArrival {
						t.Fatalf("race error = %v, envelope=%#v", result.err, envelope)
					}
				}
			}
			if _, err := driveNativePublishedJoinCompletionForTest(t, fixture, h.mutations, pc, memberCtx, mustPipelineNode("orders", "join-node"), handler, contractHandlerExecutionResult{}); err != nil {
				t.Fatal(err)
			}
			instance, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !ok {
				t.Fatalf("load final instance = %v, %v", ok, err)
			}
			if len(instance.TransitionHistory) != 1 {
				t.Fatalf("persisted transition winners = %d, want 1: %#v", len(instance.TransitionHistory), instance.TransitionHistory)
			}
			finalCarrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			closed, ok, err := joinruntime.Load(finalCarrier.StateBuckets, mustPipelineNode("orders", "join-node"), joinruntime.ActivationKey(ref))
			if err != nil || !ok || closed.Status != joinruntime.StatusClosed {
				t.Fatalf("closed activation = %#v, %v, %v", closed, ok, err)
			}
			if closed.CloseReason == joinruntime.CloseReasonComplete && instance.CurrentState != "ready" {
				t.Fatalf("complete close state = %s", instance.CurrentState)
			}
			if closed.CloseReason == joinruntime.CloseReasonDeadline && instance.CurrentState != "attention" {
				t.Fatalf("timeout close state = %s", instance.CurrentState)
			}
			_, cancellations := h.mutations.schedules()
			if closed.CloseReason == joinruntime.CloseReasonComplete {
				assertJoinCompletionCancellationsForTest(t, cancellations, ref)
			} else if len(cancellations) != 1 || cancellations[0].Command.TaskID != handle.TaskID() {
				t.Fatalf("deadline cancelled unrelated obligations: count=%d", len(cancellations))
			}
			for _, cancellation := range cancellations {
				found := false
				for _, wakeup := range h.schedules.activationIDs {
					found = found || wakeup == cancellation.ID
				}
				if !found {
					t.Fatalf("close omitted exact cancellation wakeup %s", cancellation.ID)
				}
			}
		})
	}
}

func VerifyNativeWorkflowJoinArmArrivalRaceIsEarlyOrAdmittedOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			h := newNativeExactWorkflowJoinHarness(t, tc.name, "orders", "dispatching", []any{"a", "b"}, workflowJoinLifecycleBundleStartingAt(t, "dispatching"), open)
			store, ctx, pc := h.store, h.ctx, h.pc
			path, entityID := h.path, h.entityID
			if err := applyTestInitialEntryEffect(ctx, pc, h.route, entityID); err != nil {
				t.Fatal(err)
			}
			handler := pc.SemanticSource().ExecutableNodeEventHandlers(mustPipelineNode("orders", "join-node"))["item.completed"]
			arrival := nativeWorkflowJoinEventForTest(ctx, "orders", path, entityID, "item.completed", json.RawMessage(`{"member_id":"a","result":{"ok":true}}`), time.Now().UTC())
			triggerState := mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(path), entityID)
			start := make(chan struct{})
			armErr := make(chan error, 1)
			arrivalErr := make(chan error, 1)
			go func() {
				<-start
				event := nativeWorkflowJoinEventForTest(ctx, "orders", path, entityID, "dispatch.completed", []byte("{}"), time.Now().UTC())
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPipelineNode("orders", "dispatcher")), Target: events.MustExistingEntityTarget(event.Envelope().Target)}
				published, err := nativeWorkflowJoinPublicationContextForTest(t, h.fixture, pc, ctx, event, route, true)
				if err == nil {
					_, err = pc.dispatchWorkflowNodeEventResult(published, event)
				}
				armErr <- err
			}()
			go func() {
				<-start
				_, err := executeNativePublishedWorkflowJoinForTest(t, h.fixture, h.mutations, pc, ctx, mustPipelineNode("orders", "join-node"), handler, workflowTriggerContext{Event: arrival, State: triggerState, HandlerEventKey: "item.completed"})
				arrivalErr <- err
			}()
			close(start)
			armFailure, arrivalFailure := <-armErr, <-arrivalErr
			if armFailure != nil {
				t.Fatalf("arm: %v", armFailure)
			}
			err := arrivalFailure
			if err != nil {
				envelope, ok := runtimefailures.EnvelopeFromError(err)
				if !ok || envelope.Class != runtimefailures.ClassEarlyArrival {
					t.Fatalf("arrival error = %v, envelope=%#v", err, envelope)
				}
			}
			instance, ok, loadErr := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if loadErr != nil || !ok {
				t.Fatalf("load = %v, %v", ok, loadErr)
			}
			carrier, loadErr := workflowInstanceStateCarrier(instance)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			activation, ok, loadErr := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey(t, carrier.StateBuckets, mustPipelineNode("orders", "join-node")))
			if loadErr != nil || !ok || activation.Status != joinruntime.StatusOpen {
				t.Fatalf("activation = %#v, %v, %v", activation, ok, loadErr)
			}
			if activation.Completed() < 0 || activation.Completed() > 1 {
				t.Fatalf("completed = %d, want early 0 or admitted 1", activation.Completed())
			}
			if (err == nil) != (activation.Completed() == 1) {
				t.Fatalf("arrival err=%v completed=%d; want exact early/admitted alternatives", err, activation.Completed())
			}
			upserts, _ := h.mutations.schedules()
			if len(upserts) != 1 {
				t.Fatalf("schedule intents = %d, want 1", len(upserts))
			}
		})
	}
}

func VerifyNativeWorkflowJoinPersistedArrivalClassificationOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			h := newNativeExactWorkflowJoinHarness(t, tc.name, "orders", "dispatching", []any{"a", "b"}, workflowJoinLifecycleBundleStartingAt(t, "dispatching"), open)
			store, ctx, pc := h.store, h.ctx, h.pc
			path, entityID := h.path, h.entityID
			if err := applyTestInitialEntryEffect(ctx, pc, h.route, entityID); err != nil {
				t.Fatal(err)
			}
			handler := pc.SemanticSource().ExecutableNodeEventHandlers(mustPipelineNode("orders", "join-node"))["item.completed"]
			memberEvent := func(id, member, result string) events.Event {
				return nativeWorkflowJoinEventForTest(ctx, "orders", path, entityID, "item.completed", mustJSON(map[string]any{"member_id": member, "result": map[string]any{"value": result}}), time.Now().UTC())
			}
			deliver := func(coordinator *PipelineCoordinator, id, member, result string) error {
				evt := memberEvent(id, member, result)
				_, err := executeNativePublishedWorkflowJoinForTest(t, h.fixture, h.mutations, coordinator, ctx, mustPipelineNode("orders", "join-node"), handler, workflowTriggerContext{Event: evt, State: mustCurrentWorkflowState(t, coordinator, ctx, testWorkflowInstanceRoute(path), entityID), HandlerEventKey: "item.completed"})
				return err
			}
			assertClass := func(err error, want runtimefailures.Class) {
				t.Helper()
				envelope, ok := runtimefailures.EnvelopeFromError(err)
				if err == nil || !ok || envelope.Class != want {
					t.Fatalf("error = %v, envelope=%#v, want %s", err, envelope, want)
				}
			}

			assertClass(deliver(pc, "early", "a", "one"), runtimefailures.ClassEarlyArrival)
			h.transition("awaiting", "dispatch.completed")
			dispatched, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !found || len(dispatched.TransitionHistory) != 1 || dispatched.TransitionHistory[0].From != "dispatching" || dispatched.TransitionHistory[0].To != "awaiting" {
				t.Fatalf("dispatch transition evidence = %+v, %v", dispatched, err)
			}
			assertClass(deliver(pc, "unexpected", "c", "other"), runtimefailures.ClassUnexpectedArrival)
			if err := deliver(pc, "a-first", "a", "one"); err != nil {
				t.Fatal(err)
			}
			h.restart()
			store, ctx, pc = h.store, h.ctx, h.pc
			if err := deliver(pc, "a-exact", "a", "one"); err != nil {
				t.Fatal(err)
			}
			assertClass(deliver(pc, "a-conflict", "a", "changed"), runtimefailures.ClassConflictingDuplicate)
			// This publication is admitted before closure and delivered afterwards;
			// fresh ingress after leaving the stage would instead be early.
			late := memberEvent("b-stale", "b", "two")
			lateCtx, err := nativeWorkflowJoinPublicationContextForTest(t, h.fixture, pc, ctx, late, events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(mustPipelineNode("orders", "join-node")),
				Target:    events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "orders", FlowInstance: path, EntityID: entityID}),
			}, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := deliver(pc, "b-complete", "b", "two"); err != nil {
				t.Fatal(err)
			}
			_, err = executeNativeClaimedPipelineHandlerForTest(t, pc, lateCtx, mustPipelineNode("orders", "join-node"), handler, workflowTriggerContext{Event: late, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(path), entityID), HandlerEventKey: "item.completed"})
			assertClass(err, runtimefailures.ClassStaleArrival)

			instance, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !ok {
				t.Fatalf("load = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			closed, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey(t, carrier.StateBuckets, mustPipelineNode("orders", "join-node")))
			if err != nil || !ok || closed.Status != joinruntime.StatusClosed || closed.Completed() != 2 {
				t.Fatalf("closed activation = %#v, %v, %v", closed, ok, err)
			}
			results, err := closed.Results()
			if err != nil {
				t.Fatal(err)
			}
			if len(results) != 2 || results[0].(map[string]any)["value"] != "one" || results[1].(map[string]any)["value"] != "two" {
				t.Fatalf("persisted results = %#v, want membership order", results)
			}
			facts, err := closed.Context()
			if err != nil || facts["completed"] != 2 {
				t.Fatalf("persisted join context = %#v err=%v", facts, err)
			}
			if instance.CurrentState != "ready" || len(instance.TransitionHistory) != 1 || instance.Revision <= dispatched.Revision || instance.TransitionHistory[0].From != "awaiting" || instance.TransitionHistory[0].To != "ready" {
				t.Fatalf("final lifecycle = state:%s history:%#v", instance.CurrentState, instance.TransitionHistory)
			}
			_, cancellations := h.mutations.schedules()
			assertJoinCompletionCancellationsForTest(t, cancellations, closed.JoinRef())
			if len(h.schedules.activationIDs) == 0 {
				t.Fatal("committed schedule evidence was not reconciled through the lifecycle owner")
			}
		})
	}
}

func VerifyNativeWorkflowJoinExpectedZeroCompletesAfterRestartOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			h := newNativeExactWorkflowJoinHarness(t, tc.name, "orders", "dispatching", []any{}, nil, open)
			fixture, pc, ctx, store := h.fixture, h.pc, h.ctx, h.store
			path, entityID := h.path, h.entityID
			dispatchHandler := pc.SemanticSource().ExecutableNodeEventHandlers(mustPipelineNode("orders", "dispatcher"))["order.accepted"]
			dispatch := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("fan-out-empty"), events.EventType("order.accepted"), "", "", json.RawMessage(`{"line_items":[]}`), 0, runtimecorrelation.RunIDFromContext(ctx), h.envelope(), testWorkflowRoutingSource("orders", path, entityID), time.Now().UTC())
			if dispatchHandler.FanOut == nil {
				t.Fatal("dispatcher fixture lost fan_out")
			}
			dispatchNode := mustPipelineNode("orders", "dispatcher")
			route := nativeFanOutTriggerRouteForTest(t, fixture, pc, ctx, dispatch, dispatchNode)
			handled, err := pc.executeNodeHandlerPlanResult(withWorkflowNodeDeliveryRoute(ctx, route), dispatchNode, dispatch)
			if err != nil || !handled {
				t.Fatalf("empty fan_out = handled:%v err:%v", handled, err)
			}
			committedSchedules, _ := h.mutations.schedules()
			if len(committedSchedules) != 1 || committedSchedules[0].Command.EventType != joinCompleteEvent {
				t.Fatalf("closed-operation completion schedules = %#v", committedSchedules)
			}
			armed, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !ok {
				t.Fatalf("load armed zero join = %v, %v", ok, err)
			}
			if len(armed.TransitionHistory) != 1 || armed.TransitionHistory[0].TriggerEventID != dispatch.ID() || armed.TransitionHistory[0].From != "dispatching" || armed.TransitionHistory[0].To != "awaiting" {
				t.Fatalf("empty dispatch lost exact transition: %+v", armed)
			}
			armedCarrier, err := workflowInstanceStateCarrier(armed)
			if err != nil {
				t.Fatal(err)
			}
			if err := pc.reconcileClosedJoinSchedules(ctx, testWorkflowInstanceRoute(path), entityID, armedCarrier); err != nil {
				t.Fatalf("reconcile pending zero join: %v", err)
			}
			schedule := committedSchedules[0]
			h.restart()
			fixture, pc, ctx, store = h.fixture, h.pc, h.ctx, h.store
			fire := workflowJoinScheduleEventForTest(t, "join-zero-fire", schedule, runtimecorrelation.RunIDFromContext(ctx), h.envelope(), time.Now().UTC())
			result, err := executeNativeResolvedJoinForTest(t, fixture, pc, ctx, fire, workflowTriggerContext{Event: fire, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(path), entityID)})
			if err != nil || !result.Handled {
				t.Fatalf("completion fire = handled:%v err:%v", result.Handled, err)
			}
			fired, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !ok || len(fired.TransitionHistory) != 1 || fired.TransitionHistory[0].TriggerEventID != fire.ID() {
				t.Fatalf("completion lost its exact occurrence: %+v, %v", fired, err)
			}
			if _, err := executeNativeResolvedJoinForTest(t, fixture, pc, ctx, fire, workflowTriggerContext{Event: fire, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(path), entityID)}); err != nil {
				t.Fatalf("duplicate completion fire: %v", err)
			}
			instance, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !ok {
				t.Fatalf("load = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey(t, carrier.StateBuckets, mustPipelineNode("orders", "join-node")))
			if err != nil || !ok || !activation.OutcomeFired || activation.OutcomePending || !activation.TimerCancelled {
				t.Fatalf("zero activation = %#v, %v, %v", activation, ok, err)
			}
			if instance.CurrentState != "ready" || !reflect.DeepEqual(instance.TransitionHistory, fired.TransitionHistory) || instance.TransitionHistory[0].From != "awaiting" || instance.TransitionHistory[0].To != "ready" {
				t.Fatalf("zero completion lifecycle = state:%s revision:%d armed:%d fire:%s history:%#v", instance.CurrentState, instance.Revision, armed.Revision, fire.ID(), instance.TransitionHistory)
			}
			_, cancellations := h.mutations.schedules()
			if len(cancellations) != 1 || cancellations[0].Command.TaskID != schedule.Command.TaskID {
				t.Fatalf("zero completion must cancel its actual completion, not invent a never-admitted deadline: %#v", cancellations)
			}
			deadline, err := timeridentity.JoinTimeoutHandle(activation.JoinRef())
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]bool{deadline.TaskID(): true, schedule.Command.TaskID: true}
			requested := h.mutations.requestedCancellations()
			if len(requested) != 2 {
				t.Fatalf("completion cancellation commands=%d, want both exact handles", len(requested))
			}
			for _, request := range requested {
				if !want[request.Command.TaskID] {
					t.Fatalf("duplicate or foreign cancellation requested: %s", request.Command.TaskID)
				}
				delete(want, request.Command.TaskID)
			}
		})
	}
}

func VerifyNativeWorkflowJoinExpectedZeroStageExitCancelsPendingCompletionOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			h := newNativeExactWorkflowJoinHarness(t, tc.name, "orders", "awaiting", []any{}, nil, open)
			fixture, pc, ctx, store := h.fixture, h.pc, h.ctx, h.store
			path, entityID := h.path, h.entityID
			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute(path), entityID); err != nil {
				t.Fatalf("arm zero join: %v", err)
			}
			upserts, _ := h.mutations.schedules()
			if len(upserts) != 1 || upserts[0].Command.EventType != joinCompleteEvent {
				t.Fatalf("completion schedules = %#v", upserts)
			}
			completion := upserts[0]
			h.transition("dispatching", "manual.abort")
			_, cancellations := h.mutations.schedules()
			if len(cancellations) != 1 || cancellations[0].Command.EventType != joinCompleteEvent {
				t.Fatalf("completion cancellations = %#v", cancellations)
			}

			instance, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !ok {
				t.Fatalf("load exited instance = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey(t, carrier.StateBuckets, mustPipelineNode("orders", "join-node")))
			if err != nil || !ok || activation.Status != joinruntime.StatusClosed || activation.CloseReason != joinruntime.CloseReasonStageExit || activation.OutcomePending || activation.OutcomeFired || !activation.TimerCancelled {
				t.Fatalf("exited zero activation = %#v, %v, %v", activation, ok, err)
			}

			fire := workflowJoinScheduleEventForTest(t, "join-zero-after-exit", completion, runtimecorrelation.RunIDFromContext(ctx), h.envelope(), time.Now().UTC())
			result, err := executeNativeResolvedJoinForTest(t, fixture, pc, ctx, fire, workflowTriggerContext{Event: fire, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(path), entityID)})
			if err != nil || result.Handled {
				t.Fatalf("late discarded completion fire = handled:%v err:%v, want unhandled", result.Handled, err)
			}
			instance, ok, err = store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !ok || instance.CurrentState != "dispatching" || len(instance.TransitionHistory) != 1 {
				t.Fatalf("lifecycle after late completion = instance:%#v found:%v err:%v", instance, ok, err)
			}
			_, afterLateCancellations := h.mutations.schedules()
			if len(afterLateCancellations) != 1 {
				t.Fatalf("late completion repeated cancellation: %#v", afterLateCancellations)
			}
		})
	}
}

func VerifyNativeWorkflowJoinFailurePersistsCanonicalDeliveryOutcomeAndRuntimeLogForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := workflowJoinLifecycleBundleStartingAt(t, "dispatching")
			fixture, pc, ctx, _ := nativeWorkflowJoinCoordinatorForTest(t, backend, bundle, nil, open)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{InstanceID: strings.TrimPrefix(path, "orders/"), StorageRef: path, WorkflowName: "orders", WorkflowVersion: bundle.WorkflowVersion(), CurrentState: "dispatching", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"expected": []any{"a"}, "instance_key": strings.TrimPrefix(path, "orders/")},
				EntityType: "test_entity"})); err != nil {
				t.Fatal(err)
			}
			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute(path), entityID); err != nil {
				t.Fatal(err)
			}
			evt := nativeWorkflowJoinEventForTest(ctx, "orders", path, entityID, "orders/item.completed", []byte(`{"member_id":"a","result":{"ok":true}}`), time.Now().UTC())
			route := publishNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, evt, "join-node")
			if resolved := workflowNodeEventHandlerResolutionForDelivery(pc.SemanticSource(), mustPipelineNode("orders", "join-node"), evt); !resolved.Matched {
				t.Fatalf("join handler did not resolve: %#v", resolved)
			}
			if _, err := fixture.Store.ProveHandoff(ctx, evt.ID(), route); err != nil {
				t.Fatalf("seeded join delivery was not authorized: %v", err)
			}
			ctx = withWorkflowNodeDeliveryRoute(ctx, route)
			handled, err := pc.dispatchWorkflowNodeEventResult(ctx, evt)
			if !handled {
				t.Fatal("join failure was not handled")
			}
			envelope, ok := runtimefailures.EnvelopeFromError(err)
			if !ok || envelope.Class != runtimefailures.ClassEarlyArrival {
				t.Fatalf("execution failure = %v, envelope=%#v", err, envelope)
			}
			id, err := runtimedelivery.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := fixture.Store.Snapshot(ctx, id)
			outcomes, outcomesErr := fixture.Store.Outcomes(ctx, id)
			if err != nil || outcomesErr != nil || snapshot.Status != runtimedelivery.StatusDeadLetter || snapshot.Failure == nil || snapshot.Failure.Class != runtimefailures.ClassEarlyArrival || len(outcomes) != 1 || outcomes[0].Outcome != "dead_letter" {
				t.Fatalf("persisted failure = snapshot:%+v outcomes:%+v errors:%v/%v", snapshot, outcomes, err, outcomesErr)
			}
			logs := bus.runtimeLogEntries()
			if len(logs) == 0 || logs[len(logs)-1].Failure == nil || logs[len(logs)-1].Failure.Class != runtimefailures.ClassEarlyArrival {
				t.Fatalf("runtime logs = %#v", logs)
			}
		})
	}
}
func workflowJoinLifecycleBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return workflowJoinLifecycleBundleWithReview(t, false)
}

func workflowJoinLifecycleBundleStartingAt(t *testing.T, stage string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	files := workflowJoinLifecycleFixtureFiles(false, "")
	for _, name := range []string{"schema.yaml", "orders/schema.yaml"} {
		line := "  " + stage + ": {}\n"
		if strings.Count(files[name], line) != 1 {
			t.Fatalf("%s: missing exact entry declaration %s", name, stage)
		}
		files[name] = strings.Replace(files[name], line, "", 1)
		files[name] = strings.Replace(files[name], "stages:\n", "stages:\n"+line, 1)
	}
	return loadWorkflowTempBundle(t, files)
}

func workflowJoinLifecycleBundleWithReview(t *testing.T, review bool) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return workflowJoinLifecycleBundleWithOptions(t, review, "")
}

func workflowJoinLifecycleBundleWithOptions(t *testing.T, review bool, loop string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle := loadWorkflowTempBundle(t, workflowJoinLifecycleFixtureFiles(review, loop))
	// Existing fixture consumers select the child plan for explicit scope tests.
	for i, plan := range bundle.Semantics.Joins {
		if plan.Node.FlowPath() == "orders" {
			bundle.Semantics.Joins[0], bundle.Semantics.Joins[i] = bundle.Semantics.Joins[i], bundle.Semantics.Joins[0]
			break
		}
	}
	return bundle
}

func workflowJoinLifecycleFixtureFiles(review bool, loop string) map[string]string {
	files := map[string]string{
		"schema.yaml":   "name: workflow-join-lifecycle\n",
		"entities.yaml": "test_entity: {}\n",
		"orders/schema.yaml": `name: orders
instance: instance_key
stages:
  awaiting: {}
  dispatching: {}
  ready: {final: true}
  attention: {final: true}
`,
		"orders/entities.yaml": "test_entity:\n  instance_key: {type: text, _unused_reason: fixture instance identity}\n  expected: list<text>\n",
		"orders/types.yaml":    "types:\n  ItemResult:\n    ok: boolean\n  LineItem:\n    id: text\n",
		"orders/events.yaml": `item.completed:
  member_id: text
  result: ItemResult
order.accepted:
  line_items: list<LineItem>
line_item.requested:
  line_item_id: text
dispatch.completed:
manual.abort:
`,
		"orders/nodes.yaml": `dispatcher:
  execution_type: system_node
  event_handlers:
    order.accepted:
      fan_out:
        items_from: payload.line_items
        as: line_item
        identity: line_item.id
        emit:
          event: line_item.requested
          fields:
            line_item_id: line_item.id
      advances_to: awaiting
    dispatch.completed:
      advances_to: awaiting
    manual.abort:
      advances_to: dispatching
join-node:
  execution_type: system_node
  event_handlers:
    item.completed:
      join:
        id: awaiting
        stage: awaiting
        members: {from: state.expected, by: payload.member_id}
        output: payload.result
        on_complete: {advances_to: ready}
        deadline: {after: 1h, from: stage_entry}
        on_deadline: {advances_to: attention}
`,
	}
	if review {
		files["orders/schema.yaml"] = strings.Replace(files["orders/schema.yaml"], "  awaiting: {}", "  awaiting: {}\n  reviewing: {}", 1)
		files["orders/events.yaml"] += "review.requested:\napproval.completed:\n  member_id: text\n  result: ItemResult\n"
		files["orders/nodes.yaml"] = strings.Replace(files["orders/nodes.yaml"], "    manual.abort:", "    review.requested:\n      advances_to: reviewing\n    manual.abort:", 1)
		files["orders/nodes.yaml"] = strings.Replace(files["orders/nodes.yaml"], "id: awaiting", "id: shared", 1)
		files["orders/nodes.yaml"] += `    approval.completed:
      join:
        id: shared
        stage: reviewing
        members: {from: state.expected, by: payload.member_id}
        output: payload.result
        on_complete: {advances_to: ready}
        deadline: {after: 1h, from: stage_entry}
        on_deadline: {advances_to: attention}
`
	}
	if loop != "" {
		files["orders/schema.yaml"] = strings.Replace(files["orders/schema.yaml"], "  ready: {final: true}", "  ready: {}", 1)
		files["orders/schema.yaml"] += "loops:\n  revision:\n    revision_field: revision_id\n    max_attempts: 3\n    escape: {advances_to: attention}\n"
		from := "awaiting"
		if loop == "reentrant" || loop == "captured" {
			if loop == "reentrant" {
				from = "ready"
			}
		}
		files["orders/nodes.yaml"] = strings.Replace(files["orders/nodes.yaml"], "    item.completed:\n      join:", "    item.completed:\n      loop: {admit: revision, from: awaiting}\n      join:", 1)
		if loop == "captured" {
			for _, replacement := range []struct{ old, new string }{
				{"on_complete: {advances_to: ready}", "on_complete:\n          advances_to: awaiting\n          data_accumulation:\n            writes:\n              - target_field: expected\n                value: |-\n                  [loop.revision_id]"},
				{"on_deadline: {advances_to: attention}", "on_deadline:\n          advances_to: awaiting\n          data_accumulation:\n            writes:\n              - target_field: expected\n                value: |-\n                  [loop.revision_id]"},
			} {
				files["orders/nodes.yaml"] = strings.Replace(files["orders/nodes.yaml"], replacement.old, replacement.new, 1)
			}
		}
		files["orders/events.yaml"] += "loop.start:\nloop.repeat:\n  revision_id: text\n"
		files["orders/events.yaml"] = strings.Replace(files["orders/events.yaml"], "item.completed:\n", "item.completed:\n  revision_id: text\n", 1)
		files["orders/nodes.yaml"] += "observer:\n  execution_type: system_node\n  event_handlers:\n    loop.start:\n      loop: {start: revision, from: dispatching}\n      advances_to: awaiting\n    loop.repeat:\n      loop: {repeat: revision, from: " + from + "}\n      advances_to: awaiting\n"
	}
	// Root and child executions use independently compiled declarations, even
	// when the fixture deliberately gives them identical local names.
	for _, name := range []string{"schema.yaml", "entities.yaml", "events.yaml", "types.yaml", "nodes.yaml"} {
		files[name] = files["orders/"+name]
	}
	files["schema.yaml"] = strings.Replace(files["schema.yaml"], "name: orders", "name: workflow-join-lifecycle", 1)
	files["schema.yaml"] = strings.Replace(files["schema.yaml"], "instance: instance_key\n", "", 1)
	return files
}

func workflowJoinTestEnvelope(instancePath, entityID string) events.EventEnvelope {
	return events.EnvelopeForSourceRoute(
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), instancePath),
		events.RouteIdentity{FlowID: "orders", FlowInstance: instancePath, EntityID: entityID},
	)
}
