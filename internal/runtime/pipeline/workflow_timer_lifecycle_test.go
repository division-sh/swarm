package pipeline

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

type recordingGenericScheduleWakeupOwner struct {
	activationIDs []string
}

func (s *recordingGenericScheduleWakeupOwner) ReconcileWakeupWithRecovery(_ context.Context, activationID string) (bool, error) {
	s.activationIDs = append(s.activationIDs, activationID)
	return false, nil
}

func VerifyNativeExecuteNodeHandlerPlan_DoesNotRunOtherNodeHandlerForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
			fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-child-flow-absolute-path")
			platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
			if err != nil {
				t.Fatalf("load bundle: %v", err)
			}
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			entityID := runID
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID:      runID,
				StorageRef:      runID,
				EntityID:        entityID,
				WorkflowName:    ".",
				WorkflowVersion: bundle.WorkflowVersion(),
				CurrentState:    "waiting",
				Fields:          map[string]any{},
				EntityType:      "test_entity",
			})); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}

			envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), runID)
			envelope = events.EnvelopeForSourceRoute(envelope, events.RouteIdentity{
				FlowID: "child", FlowInstance: "child", EntityID: entityID,
			})
			envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{
				FlowID: ".", FlowInstance: runID, EntityID: entityID,
			})
			evt := eventtest.ExistingRunRootIngressWithRoutingSource(
				uuid.NewString(),
				events.EventType("child/task.done"),
				"cataloge2e",
				"",
				[]byte(`{"entity_id":"`+entityID+`"}`),
				0,
				runID,
				envelope,
				eventtest.StaticFlowRoutingSource("child", "child", entityID),
				time.Now().UTC(),
			)

			route := workflowNodeStampedConnectRouteForHandlerEvent(t, pc.SemanticSource(), "task.done", "listener")
			route.Target = events.MustExistingEntityTarget(evt.TargetRoute())
			if err := fixture.PublishNode(ctx, evt, route); err != nil {
				t.Fatal(err)
			}
			deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, route)

			if handled := pc.executeNodeHandlerPlan(deliveryCtx, pipelineNode(t, "", "dispatcher"), evt); handled {
				t.Fatal("dispatcher should not handle child/task.done")
			}
			instance, ok, err := pc.workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
			if err != nil {
				t.Fatalf("load workflow instance after wrong node execution: %v", err)
			}
			if !ok {
				t.Fatal("workflow instance missing after wrong node execution")
			}
			if got := instance.CurrentState; got != "waiting" {
				t.Fatalf("state after wrong node execution = %q, want waiting", got)
			}

			if handled, err := pc.executeNodeHandlerPlanResult(deliveryCtx, pipelineNode(t, "", "listener"), evt); err != nil || !handled {
				t.Fatalf("listener should handle child/task.done: handled=%v err=%v", handled, err)
			}
			instance, ok, err = pc.workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
			if err != nil {
				t.Fatalf("load workflow instance after listener execution: %v", err)
			}
			if !ok {
				t.Fatal("workflow instance missing after listener execution")
			}
			if got := instance.CurrentState; got != "done" {
				t.Fatalf("state after listener execution = %q, want done", got)
			}
		})
	}
}
func VerifyNativeExecuteNodeHandlerPlan_PreservesRootStateForChildFlowTransitionsForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
			fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-child-flow-pin-wiring")
			platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
			if err != nil {
				t.Fatalf("load bundle: %v", err)
			}
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			entityID := runID
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID:      runID,
				StorageRef:      runID,
				EntityID:        entityID,
				WorkflowName:    ".",
				WorkflowVersion: bundle.WorkflowVersion(),
				CurrentState:    "ready",
				Fields:          map[string]any{},
				EntityType:      "test_entity",
			})); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}

			childEntityID := FlowInstanceEntityID("child")
			constructNativePipelineScenarioForTest(t, fixture, pc, ctx, "child")
			triggerEnvelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, childEntityID), "child")
			triggerEnvelope = events.EnvelopeForTargetRoute(triggerEnvelope, events.RouteIdentity{
				FlowID: "child", FlowInstance: "child", EntityID: childEntityID,
			})
			trigger := eventtest.ExistingRunRootIngress(
				uuid.NewString(),
				events.EventType("child/work.requested"),
				"cataloge2e",
				"",
				[]byte(`{"entity_id":"`+entityID+`"}`),
				0,
				runID,
				triggerEnvelope,
				time.Now().UTC(),
			)

			childWorker := pipelineSourceNode(t, pc.SemanticSource(), "child", "child-worker")
			triggerRoute := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(childWorker),
				Target: events.MustExistingEntityTarget(events.RouteIdentity{
					FlowID: "child", FlowInstance: "child", EntityID: childEntityID,
				}),
			}
			if err := fixture.PublishNode(ctx, trigger, triggerRoute); err != nil {
				t.Fatal(err)
			}

			gate := &nativePipelineDeliveryDispatchGateForTest{Bus: pc.bus, EngineMutationPublicationPlanner: bus, entered: make(chan struct{}), release: make(chan struct{}), flowScope: "child"}
			pc.bus = gate
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate.release) }) }
			done := make(chan struct{})
			var childHandled bool
			var childErr error
			go func() {
				defer close(done)
				childHandled, childErr = pc.executeNodeHandlerPlanResult(withWorkflowNodeDeliveryRoute(ctx, triggerRoute), childWorker, trigger)
			}()
			t.Cleanup(func() {
				release()
				join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				select {
				case <-done:
				case <-join.Done():
					t.Error("native child execution did not join after releasing its dispatch gate")
				}
			})
			cut, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			select {
			case <-gate.entered:
			case <-done:
				t.Fatalf("child stopped before its native dispatch cut: %t/%v", childHandled, childErr)
			case <-cut.Done():
				t.Fatal("native child never reached its post-commit dispatch cut")
			}
			instance, ok, err := pc.workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
			if err != nil {
				t.Fatalf("load workflow instance after child-worker execution: %v", err)
			}
			if !ok {
				t.Fatal("workflow instance missing after child-worker execution")
			}
			if got := instance.CurrentState; got != "ready" {
				t.Fatalf("root state after child-worker execution = %q, want ready", got)
			}

			parentListener := pipelineNode(t, "", "parent-listener")
			handler, ok := pc.SemanticSource().ExecutableNodeEventHandler(parentListener, "work.completed")
			if !ok {
				t.Fatal("parent-listener handler missing for root-local work.completed")
			}
			if !handler.Emit.Empty() || !handler.OnSuccess.Empty() {
				t.Fatalf("parent-listener carries retired dead output: emit=%#v on_success=%#v", handler.Emit, handler.OnSuccess)
			}

			release()
			select {
			case <-done:
			case <-cut.Done():
				t.Fatal("native child/parent dispatch did not join")
			}
			if !childHandled || childErr != nil {
				t.Fatalf("child-worker native execution: %t/%v", childHandled, childErr)
			}
			if bus.publishedCount() != 1 {
				t.Fatalf("child completion publications=%d, want one", bus.publishedCount())
			}
			completion := bus.persistedPublishedEvent(t, fixture, ctx, 0)
			parent, err := fixture.NodeDeliverySnapshot(ctx, runID, completion.ID(), parentListener.Key())
			if err != nil {
				t.Fatalf("exact native parent delivery event=%s type=%s: %v", completion.ID(), completion.Type(), err)
			}
			if target := parent.Route.Target.Route(); target.FlowInstance != runID || target.EntityID != entityID {
				t.Fatalf("native parent delivery targeted another instance: %+v", target)
			}
			if _, err := fixture.Store.ProveHandoff(ctx, completion.ID(), parent.Route); err != nil {
				t.Fatalf("child completion lost its exact original parent handoff: %v", err)
			}
			instance, ok, err = pc.workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
			if err != nil {
				t.Fatalf("load workflow instance after parent-listener execution: %v", err)
			}
			if !ok {
				t.Fatal("workflow instance missing after parent-listener execution")
			}
			if got := instance.CurrentState; got != "done" {
				t.Fatalf("root state after parent-listener execution = %q, want done", got)
			}
		})
	}
}
func VerifyNativePipelineIntercept_HandlesChildFlowOutputForRootListenerForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
			fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-child-flow-pin-wiring")
			platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
			if err != nil {
				t.Fatalf("load bundle: %v", err)
			}
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			entityID := runID
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID:      runID,
				StorageRef:      runID,
				EntityID:        entityID,
				WorkflowName:    ".",
				WorkflowVersion: bundle.WorkflowVersion(),
				CurrentState:    "ready",
				Fields:          map[string]any{},
				EntityType:      "test_entity",
			})); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}

			completion := eventtest.ExistingRunRootIngress(
				uuid.NewString(),
				events.EventType("child/work.completed"),
				"cataloge2e",
				"",
				[]byte(`{"entity_id":"`+entityID+`"}`),
				0,
				runID,
				events.EnvelopeForTargetRoute(
					events.EventEnvelope{},
					events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID},
				),
				time.Now().UTC(),
			)

			route := workflowNodeStampedConnectRouteForHandlerEvent(t, pc.SemanticSource(), "work.completed", "parent-listener")
			route.Target = events.MustExistingEntityTarget(completion.TargetRoute())
			if err := fixture.PublishNode(ctx, completion, route); err != nil {
				t.Fatal(err)
			}
			passThrough, emitted, _, err := pc.Intercept(withWorkflowNodeDeliveryRoute(ctx, route), completion)
			if err != nil {
				t.Fatalf("Intercept: %v", err)
			}
			if passThrough {
				t.Fatal("expected the exact parent-listener delivery to be consumed without event-wide passthrough")
			}
			if len(emitted) != 0 {
				t.Fatalf("emitted = %#v, want no retired dead output", emitted)
			}
		})
	}
}
func VerifyNativePipelineCoordinatorIntercept_NestedDescendantCompletionDoesNotEmitChildContinuationForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
			fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
			platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
			if err != nil {
				t.Fatalf("load bundle: %v", err)
			}
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			root := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, ".")
			childInstance := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, "child")
			rootEntityID := root.EntityID
			grandchild := constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, "child/grandchild")
			grandchild.CurrentState = "finished"
			if err := fixture.Construct(ctx, grandchild); err != nil {
				t.Fatal(err)
			}
			grandchildEntityID := grandchild.EntityID
			completion := eventtest.ExistingRunRootIngress(
				uuid.NewString(),
				events.EventType("child/grandchild/micro.done"),
				"cataloge2e",
				"",
				[]byte(`{"entity_id":"`+grandchildEntityID+`"}`),
				0,
				runID,
				events.EnvelopeForTargetRoute(
					events.EnvelopeForEntityID(events.EventEnvelope{}, grandchildEntityID),
					events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: rootEntityID},
				),
				time.Now().UTC(),
			)

			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineNode(t, ".", "root-collector")), Target: events.MustExistingEntityTarget(completion.TargetRoute())}
			if err := fixture.PublishNode(ctx, completion, route); err != nil {
				t.Fatal(err)
			}
			passThrough, emitted, _, err := pc.Intercept(withWorkflowNodeDeliveryRoute(ctx, route), completion)
			if err == nil || !strings.Contains(err.Error(), "stamped connect claim") {
				t.Fatalf("Intercept error = %v, want stamped connect claim", err)
			}
			if passThrough || len(emitted) != 0 {
				t.Fatalf("failed delivery result = passThrough:%v emitted:%#v, want no output", passThrough, emitted)
			}

			child, found, err := pc.workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, childInstance.StorageRef))
			if err != nil {
				t.Fatalf("load child instance: %v", err)
			}
			if !found {
				t.Fatal("expected child instance")
			}
			if got := strings.TrimSpace(child.CurrentState); got != "waiting" {
				t.Fatalf("child current_state = %q, want waiting", got)
			}
		})
	}
}
func VerifyNativePipelineCoordinatorIntercept_NestedPackageRootConnectDoesNotAuthorizeRootResultForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
			fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
			platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
			if err != nil {
				t.Fatalf("load bundle: %v", err)
			}
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			root := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, ".")
			childInstance := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, "child")
			rootEntityID, childRowID := root.EntityID, childInstance.EntityID
			if consume, handled, err := pc.workflowNodeInterceptPolicy(ctx, "child/grandchild/micro.done", eventtest.ExistingRunRootIngress(
				"",
				events.EventType("child/grandchild/micro.done"),
				"",
				"",
				nil,
				0,
				runID,
				events.EnvelopeForTargetRoute(
					events.EnvelopeForEntityID(events.EventEnvelope{}, childRowID),
					events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: rootEntityID},
				),
				time.Time{},
			)); err != nil || handled || consume {
				t.Fatalf("workflowNodeInterceptPolicy handled = %v, consume = %v, err = %v, want no unstamped match", handled, consume, err)
			}

			completion := eventtest.ExistingRunRootIngress(
				uuid.NewString(),
				events.EventType("child/grandchild/micro.done"),
				"cataloge2e",
				"",
				[]byte(`{"entity_id":"`+childRowID+`"}`),
				0,
				runID,
				events.EnvelopeForTargetRoute(
					events.EnvelopeForEntityID(events.EventEnvelope{}, childRowID),
					events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: rootEntityID},
				),
				time.Now().UTC(),
			)

			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineNode(t, ".", "root-collector")), Target: events.MustExistingEntityTarget(completion.TargetRoute())}
			if err := fixture.PublishNode(ctx, completion, route); err != nil {
				t.Fatal(err)
			}
			passThrough, emitted, _, err := pc.Intercept(withWorkflowNodeDeliveryRoute(ctx, route), completion)
			if err == nil || !strings.Contains(err.Error(), "stamped connect claim") {
				t.Fatalf("Intercept error = %v, want stamped connect claim", err)
			}
			if passThrough || len(emitted) != 0 {
				t.Fatalf("failed delivery result = passThrough:%v emitted:%#v, want no output", passThrough, emitted)
			}
		})
	}
}
func VerifyNativePipelineCoordinatorIntercept_NestedPackageRootConnectInsideOuterSQLTxDoesNotAuthorizeRootResultForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
			fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-nested-three-levels")
			platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
			if err != nil {
				t.Fatalf("load bundle: %v", err)
			}
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			root := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, ".")
			child := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, "child")
			rootEntityID, childRowID := root.EntityID, child.EntityID
			completion := eventtest.ExistingRunRootIngress(
				uuid.NewString(),
				events.EventType("child/grandchild/micro.done"),
				"cataloge2e",
				"",
				[]byte(`{"entity_id":"`+childRowID+`"}`),
				0,
				runID,
				events.EnvelopeForTargetRoute(
					events.EnvelopeForEntityID(events.EventEnvelope{}, childRowID),
					events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: rootEntityID},
				),
				time.Now().UTC(),
			)

			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineNode(t, ".", "root-collector")), Target: events.MustExistingEntityTarget(completion.TargetRoute())}
			if err := fixture.PublishNode(ctx, completion, route); err != nil {
				t.Fatal(err)
			}
			// Ambient private SQL/context protocols are unsupported. Hold the real
			// selected coordinator instead; no transaction or admission fact escapes.
			closeCut, err := fixture.HoldUnstampedAdmissionTransaction(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := closeCut(); err != nil {
					t.Error(err)
				}
			})
			before := fixture.Transactions()
			if before.Active != 1 {
				t.Fatalf("original selected transaction cut inactive: %+v", before)
			}
			passThrough, emitted, _, err := pc.Intercept(withWorkflowNodeDeliveryRoute(ctx, route), completion)
			if err == nil || !strings.Contains(err.Error(), "stamped connect claim") {
				t.Fatalf("Intercept error = %v, want stamped connect claim", err)
			}
			if passThrough || len(emitted) != 0 {
				t.Fatalf("failed delivery result = passThrough:%v emitted:%#v, want no output", passThrough, emitted)
			}
			if during := fixture.Transactions(); during.Active != 1 || during.Claims != before.Claims {
				t.Fatalf("unstamped route borrowed the live transaction or acquired a claim: before=%+v during=%+v", before, during)
			}
			if err := closeCut(); err != nil {
				t.Fatal(err)
			}
			if after := fixture.Transactions(); after.Active != 0 || after.Claims != before.Claims {
				t.Fatalf("native transaction cut did not join cleanly: %+v", after)
			}

		})
	}
}
