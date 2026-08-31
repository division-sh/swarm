package pipeline_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type exactExternalJoinSource struct {
	semanticview.Source
	flowID string
	plans  []runtimecontracts.WorkflowJoinPlan
}

func (s exactExternalJoinSource) WorkflowJoins() []runtimecontracts.WorkflowJoinPlan {
	return append([]runtimecontracts.WorkflowJoinPlan(nil), s.plans...)
}

type joinProofGenericScheduleWakeups struct{}

func (joinProofGenericScheduleWakeups) ReconcileWakeupWithRecovery(context.Context, string) (bool, error) {
	return false, nil
}

type exactJoinScheduleLogger struct{ t *testing.T }

type exactJoinRuntimeLogger struct {
	mu      sync.Mutex
	details []string
}

func (l *exactJoinRuntimeLogger) Log(_ context.Context, _ diaglog.Level, _, _, _ string, _ string, _ string, _ string, _ string, _ string, _ map[string]string, detail any, _ *runtimefailures.Envelope, _ int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.details = append(l.details, fmt.Sprint(detail))
	return nil
}

func (l *exactJoinRuntimeLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return fmt.Sprint(l.details)
}

func (l exactJoinScheduleLogger) GenericScheduleFailure(_ context.Context, action, activationID string, err error) {
	l.t.Helper()
	l.t.Logf("generic schedule %s for %s failed: %v", action, activationID, err)
}

func (exactJoinScheduleLogger) GenericScheduleCatchupWarning(context.Context, string, int) {}

func TestWorkflowJoinDurableEventBusDeliveryClaimPreservesExactDeclarationOnBothStores(t *testing.T) {
	for _, storeCase := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		for _, flowID := range []string{"", "orders"} {
			scope := "root"
			if flowID != "" {
				scope = "flow"
			}
			t.Run(storeCase.name+"/"+scope, func(t *testing.T) {
				selected := storeCase.open(t)
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
				source := exactExternalWorkflowJoinSource(t, flowID)
				joinNode := externalPipelineSourceNode(t, source, flowID, "join-node")
				path := runID
				workflowName := source.WorkflowName()
				instanceID := runID
				if flowID != "" {
					instanceID = uuid.NewString()
					path = flowID + "/" + instanceID
					workflowName = flowID
				}
				eventType := "item.completed"
				subscriptionType := source.WorkflowName() + "/item.completed"
				var additionalDescriptors []string
				if flowID != "" {
					eventType = path + "/item.completed"
					subscriptionType = eventType
					additionalDescriptors = append(additionalDescriptors, eventType)
				}
				if owners := source.RuntimeEventOwners("item.completed"); len(owners) != 1 || !owners[0].Equal(joinNode) {
					t.Fatalf("join event owners = %#v", owners)
				}
				module := proposedEffectProofModule{
					source: source,
					workflow: runtimepipeline.NewWorkflowDefinition(workflowName, []runtimepipeline.WorkflowStage{
						{Name: "awaiting"},
						{Name: "ready", Terminal: true},
						{Name: "attention", Terminal: true},
					}, []runtimepipeline.WorkflowTransition{{
						Name: "complete-join", From: []runtimepipeline.WorkflowStateID{"awaiting"}, To: "ready",
						Trigger: "item.completed", Node: joinNode,
					}}),
					nodes: []runtimepipeline.WorkflowNode{{
						Node: joinNode, Subscriptions: []events.EventType{events.EventType(subscriptionType)},
						ExecutionType: runtimecontracts.SystemNodeExecutionType,
						Policies: map[string]runtimepipeline.WorkflowEventPolicy{
							subscriptionType: {Consume: true},
						},
					}},
				}
				probe := runtimelifecycleprobe.New()
				eventBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
					ContractBundle: source, TestLifecycleProbe: probe,
				}, additionalDescriptors...)
				if err != nil {
					t.Fatalf("new join EventBus: %v", err)
				}
				coordinator := newGateRecoveryCoordinator(eventBus, selected, runtimepipeline.PipelineCoordinatorOptions{
					Module: module, Persistence: selected.persistence,
					GenericSchedules: joinProofGenericScheduleWakeups{}, TestLifecycleProbe: probe,
				})
				eventBus.SetInterceptors(coordinator)

				route := runtimeflowidentity.RouteForInstancePath(path)
				entityID := runtimeflowidentity.EntityID(path)
				createdAt := time.Now().UTC()
				if _, err := coordinator.MaterializeInitialEntry(ctx, testRunScopedWorkflowInstanceForRun(runID, path), runtimepipeline.WorkflowInstance{
					InstanceID: instanceID, StorageRef: path, WorkflowName: workflowName, WorkflowVersion: "1.0.0",
					EntityID: entityID, CurrentState: "awaiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
					Fields:     map[string]any{"expected": []any{"a", "b"}},
					EntityType: "join_state",
				}, createdAt); err != nil {
					t.Fatalf("materialize exact join owner: %v", err)
				}
				if flowID != "" {
					if err := eventBus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: testRunScopedWorkflowInstanceForRun(runID, route.InstancePath)}); err != nil {
						t.Fatalf("add flow join route: %v", err)
					}
				}

				eventsByMember := make([]events.Event, 0, 2)
				for _, member := range []string{"a", "b"} {
					wantTargetFlow := flowID
					envelope := events.EnvelopeForFlowInstance(
						events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), workflowName,
					)
					if flowID == "" {
						wantTargetFlow = source.WorkflowName()
					} else {
						envelope = events.EnvelopeForTargetRoute(
							events.EnvelopeForFlowInstance(envelope, path),
							events.RouteIdentity{FlowID: flowID, FlowInstance: path, EntityID: entityID},
						)
					}
					event := eventtest.ExistingRunRootIngress(
						uuid.NewString(), events.EventType(eventType), "operator", "",
						[]byte(`{"member_id":"`+member+`","result":{"value":"`+member+`"}}`),
						0, runID, envelope, time.Now().UTC(),
					)
					plan, err := eventBus.CheckPublishRecipientPlan(ctx, event)
					if err != nil {
						t.Fatalf("plan member %s: %v", member, err)
					}
					if len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Recipient.ID() != joinNode.Key() {
						t.Fatalf("member %s delivery plan = %#v", member, plan)
					}
					if target := plan.DeliveryRoutes[0].Target.Route(); target.FlowID != wantTargetFlow || target.FlowInstance != path || target.EntityID != entityID {
						t.Fatalf("member %s target = %#v, want flow=%q path=%q entity=%q", member, target, wantTargetFlow, path, entityID)
					}
					if err := eventBus.PublishAcknowledged(ctx, event); err != nil {
						t.Fatalf("publish member %s: %v", member, err)
					}
					waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
						cancel()
						t.Fatalf("wait member %s quiescence: %v", member, err)
					}
					cancel()
					assertExactJoinDeliveryStatus(t, selected, ctx, event.ID(), joinNode.Key(), "delivered")
					eventsByMember = append(eventsByMember, event)
				}

				instance, found, err := coordinator.Load(ctx, testRunScopedWorkflowInstanceForRun(runID, route.InstancePath))
				if err != nil || !found || instance.CurrentState != "ready" {
					t.Fatalf("closed workflow = found:%v state:%q err:%v", found, instance.CurrentState, err)
				}
				carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.Gates, instance.StateBuckets)
				if err != nil {
					t.Fatal(err)
				}
				joins, err := joinruntime.List(carrier.StateBuckets)
				if err != nil || len(joins) != 1 {
					t.Fatalf("join readback = %#v err=%v", joins, err)
				}
				if joins[0].Status != joinruntime.StatusClosed || joins[0].CloseReason != joinruntime.CloseReasonComplete ||
					joins[0].FlowID() != flowID || joins[0].Completed() != 2 || !joins[0].TimerCancelled {
					t.Fatalf("closed join = %#v", joins[0])
				}

				beforeReplayRevision := instance.Revision
				if err := eventBus.PublishAcknowledged(ctx, eventsByMember[1]); err != nil {
					t.Fatalf("replay exact member event: %v", err)
				}
				waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
					cancel()
					t.Fatalf("wait replay quiescence: %v", err)
				}
				cancel()
				afterReplay, found, err := coordinator.Load(ctx, testRunScopedWorkflowInstanceForRun(runID, route.InstancePath))
				if err != nil || !found || afterReplay.Revision != beforeReplayRevision {
					t.Fatalf("replay mutated workflow = found:%v before:%d after:%d err:%v", found, beforeReplayRevision, afterReplay.Revision, err)
				}
				assertExactJoinDeliveryCount(t, selected, ctx, eventsByMember[1].ID(), joinNode.Key(), 1)
				assertPersistedHandlerRuleSelectionInPackage(
					t, selected, ctx, eventsByMember[1].ID(), handlerselection.ContextJoinComplete,
					handlerselection.DispositionSelected, ".", "00000000-0000-4000-8000-000000000013", "",
				)
				assertTraceHandlerRuleSelectionInPackage(
					t, selected, ctx, runID, eventsByMember[1].ID(), handlerselection.ContextJoinComplete,
					handlerselection.DispositionSelected, ".", "00000000-0000-4000-8000-000000000013", "",
				)
			})
		}
	}
}

func TestWorkflowJoinScheduleOccurrencePreservesExactDeclarationThroughDurableEventBusOnBothStores(t *testing.T) {
	outcomes := []struct {
		name          string
		expected      []any
		timeout       string
		eventName     string
		terminalState string
		closeReason   joinruntime.CloseReason
		context       handlerselection.Context
		elementID     string
	}{
		{name: "completion", expected: []any{}, timeout: "1h", eventName: "platform.join_complete", terminalState: "ready", closeReason: joinruntime.CloseReasonComplete, context: handlerselection.ContextJoinComplete, elementID: "00000000-0000-4000-8000-000000000013"},
		{name: "timeout", expected: []any{"a"}, timeout: "20ms", eventName: "platform.join_timeout", terminalState: "attention", closeReason: joinruntime.CloseReasonTimeout, context: handlerselection.ContextJoinTimeout, elementID: "00000000-0000-4000-8000-000000000014"},
	}
	for _, storeCase := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		for _, outcome := range outcomes {
			for _, flowID := range []string{"", "orders"} {
				scope := "root"
				if flowID != "" {
					scope = "flow"
				}
				t.Run(storeCase.name+"/"+outcome.name+"/"+scope, func(t *testing.T) {
					selected := storeCase.open(t)
					runtimeLogger := &exactJoinRuntimeLogger{}
					store, ok := selected.events.(interface {
						runtimegenericschedule.Store
						runtimebus.PreparedPublishEventReader
					})
					if !ok {
						t.Fatalf("selected store %T lacks generic schedule or event readback ownership", selected.events)
					}
					runID := uuid.NewString()
					insertGateRecoveryRun(t, selected, runID)
					ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
					source := exactExternalWorkflowJoinSourceWithTimeout(t, flowID, outcome.timeout)
					joinNode := externalPipelineSourceNode(t, source, flowID, "join-node")
					path := runID
					workflowName := source.WorkflowName()
					instanceID := runID
					if flowID != "" {
						instanceID = uuid.NewString()
						path = flowID + "/" + instanceID
						workflowName = flowID
					}

					module := proposedEffectProofModule{
						source: source,
						workflow: runtimepipeline.NewWorkflowDefinition(workflowName, []runtimepipeline.WorkflowStage{
							{Name: "awaiting"},
							{Name: "ready", Terminal: true},
							{Name: "attention", Terminal: true},
						}, []runtimepipeline.WorkflowTransition{
							{
								Name: "complete-join", From: []runtimepipeline.WorkflowStateID{"awaiting"}, To: "ready",
								Trigger: "item.completed", Node: joinNode,
							},
							{
								Name: "timeout-join", From: []runtimepipeline.WorkflowStateID{"awaiting"}, To: "attention",
								Trigger: "item.completed", Node: joinNode,
							},
						}),
						nodes: []runtimepipeline.WorkflowNode{{
							Node: joinNode, ExecutionType: runtimecontracts.SystemNodeExecutionType,
							Subscriptions: []events.EventType{"item.completed"},
							Policies: map[string]runtimepipeline.WorkflowEventPolicy{
								"item.completed": {Consume: true},
							},
						}},
					}
					probe := runtimelifecycleprobe.New()
					eventBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
						ContractBundle: source, TestLifecycleProbe: probe, Logger: runtimeLogger,
					}, outcome.eventName)
					if err != nil {
						t.Fatalf("new schedule occurrence EventBus: %v", err)
					}
					workOwner, ok := worklifetime.OccurrenceFromContext(ctx)
					if !ok {
						t.Fatal("join occurrence proof context lacks work owner")
					}
					scheduler := runtimepipeline.NewSchedulerWithWorkOwner(workOwner)
					lifecycle, err := runtimegenericschedule.NewLifecycle(
						store, scheduler, eventBus, eventBus.EngineDispatcher(), exactJoinScheduleLogger{t: t}, executionposture.Live,
					)
					if err != nil {
						t.Fatalf("new generic schedule lifecycle: %v", err)
					}
					t.Cleanup(func() {
						stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						if err := lifecycle.Stop(stopCtx); err != nil {
							t.Errorf("stop generic schedule lifecycle: %v", err)
						}
					})
					coordinator := newGateRecoveryCoordinator(eventBus, selected, runtimepipeline.PipelineCoordinatorOptions{
						Module: module, Persistence: selected.persistence,
						GenericSchedules: lifecycle, TestLifecycleProbe: probe,
					})
					eventBus.SetInterceptors(coordinator)

					route := runtimeflowidentity.RouteForInstancePath(path)
					entityID := runtimeflowidentity.EntityID(path)
					createdAt := time.Now().UTC()
					if _, err := coordinator.MaterializeInitialEntry(ctx, testRunScopedWorkflowInstanceForRun(runID, path), runtimepipeline.WorkflowInstance{
						InstanceID: instanceID, StorageRef: path, WorkflowName: workflowName, WorkflowVersion: "1.0.0",
						EntityID: entityID, CurrentState: "awaiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
						Fields:     map[string]any{"expected": outcome.expected},
						EntityType: "join_state",
					}, createdAt); err != nil {
						t.Fatalf("materialize immediate join owner: %v", err)
					}
					if flowID != "" {
						if err := eventBus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: testRunScopedWorkflowInstanceForRun(runID, route.InstancePath)}); err != nil {
							t.Fatalf("add flow join route: %v", err)
						}
					}

					eventID := exactJoinOccurrenceEventID(t, selected, ctx, runID, outcome.eventName)
					startedCtx, cancelStarted := context.WithTimeout(ctx, 5*time.Second)
					handlerStarted, startedErr := probe.Wait(startedCtx, runtimelifecycleprobe.Signal{
						Kind: runtimelifecycleprobe.HandlerStarted, EventID: eventID,
					})
					cancelStarted()
					if startedErr != nil {
						t.Fatalf("wait exact join occurrence handler start = %#v err=%v", handlerStarted, startedErr)
					}
					handlerCtx, cancelHandler := context.WithTimeout(ctx, 5*time.Second)
					handlerCompletion, handlerErr := probe.WaitForHandlerCompleted(handlerCtx, eventID, joinNode.Key())
					cancelHandler()
					if handlerErr != nil || handlerCompletion.Status != "completed" {
						t.Fatalf("wait exact join occurrence handler = %#v err=%v logs=%s", handlerCompletion, handlerErr, runtimeLogger.String())
					}
					waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
						cancel()
						t.Fatalf("wait occurrence delivery quiescence: %v", err)
					}
					cancel()
					assertExactJoinDeliveryStatus(t, selected, ctx, eventID, joinNode.Key(), "delivered")
					instance := waitForExactJoinState(t, ctx, coordinator, route, outcome.terminalState)
					carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.Gates, instance.StateBuckets)
					if err != nil {
						t.Fatal(err)
					}
					joins, err := joinruntime.List(carrier.StateBuckets)
					if err != nil || len(joins) != 1 {
						t.Fatalf("join occurrence readback = %#v err=%v", joins, err)
					}
					if joins[0].Status != joinruntime.StatusClosed || joins[0].FlowID() != flowID ||
						joins[0].CloseReason != outcome.closeReason || !joins[0].OutcomeFired {
						t.Fatalf("fired join occurrence = %#v", joins[0])
					}
					assertPersistedHandlerRuleSelectionInPackage(
						t, selected, ctx, eventID, outcome.context, handlerselection.DispositionSelected,
						".", outcome.elementID, "",
					)
					assertTraceHandlerRuleSelectionInPackage(
						t, selected, ctx, runID, eventID, outcome.context, handlerselection.DispositionSelected,
						".", outcome.elementID, "",
					)

					prepared, found, err := store.LoadPreparedPublishEvent(ctx, eventID)
					if err != nil || !found {
						t.Fatalf("load persisted join occurrence = found:%v err:%v", found, err)
					}
					beforeReplayRevision := instance.Revision
					if err := eventBus.PublishAcknowledged(ctx, prepared.Event.Event()); err != nil {
						t.Fatalf("replay persisted join occurrence: %v", err)
					}
					waitCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
					if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
						cancel()
						t.Fatalf("wait occurrence replay quiescence: %v", err)
					}
					cancel()
					afterReplay, found, err := coordinator.Load(ctx, testRunScopedWorkflowInstanceForRun(runID, route.InstancePath))
					if err != nil || !found || afterReplay.Revision != beforeReplayRevision {
						t.Fatalf("occurrence replay mutated workflow = found:%v before:%d after:%d err:%v", found, beforeReplayRevision, afterReplay.Revision, err)
					}
					assertExactJoinDeliveryCount(t, selected, ctx, eventID, joinNode.Key(), 1)
				})
			}
		}
	}
}

func exactExternalWorkflowJoinSource(t *testing.T, flowID string) semanticview.Source {
	return exactExternalWorkflowJoinSourceWithTimeout(t, flowID, "1h")
}

func exactExternalWorkflowJoinSourceWithTimeout(t *testing.T, flowID, timeout string) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	fixtureRoot := canonicalrouting.CopyExactJoinEventBusProofWithTimeout(t, flowID, timeout)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		fixtureRoot,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load exact join EventBus fixture: %v", err)
	}
	plans := append([]runtimecontracts.WorkflowJoinPlan(nil), bundle.Semantics.Joins...)
	if len(plans) != 1 {
		t.Fatalf("loaded exact join plans = %#v", plans)
	}
	return exactExternalJoinSource{Source: semanticview.Wrap(bundle), flowID: flowID, plans: plans}
}

func assertExactJoinDeliveryStatus(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, eventID, subscriberKey, want string) {
	t.Helper()
	query := `SELECT status, CAST(COALESCE(failure, '{}') AS TEXT) FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = ?`
	if selected.postgres {
		query = `SELECT status, COALESCE(failure, '{}'::jsonb)::text FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2`
	}
	deadline := time.Now().Add(5 * time.Second)
	var status, failure string
	var err error
	for time.Now().Before(deadline) {
		err = selected.db.QueryRowContext(ctx, query, eventID, subscriberKey).Scan(&status, &failure)
		if err == nil && status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || status != want {
		var runtimeLog string
		logQuery := `SELECT CAST(payload AS TEXT) FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at DESC LIMIT 1`
		_ = selected.db.QueryRowContext(ctx, logQuery).Scan(&runtimeLog)
		t.Fatalf("delivery %s status = %q failure=%s err=%v, want %q; runtime_log=%s", eventID, status, failure, err, want, runtimeLog)
	}
}

func assertExactJoinDeliveryCount(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, eventID, subscriberKey string, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = ?`
	if selected.postgres {
		query = `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2`
	}
	var count int
	if err := selected.db.QueryRowContext(ctx, query, eventID, subscriberKey).Scan(&count); err != nil || count != want {
		t.Fatalf("delivery %s rows = %d err=%v, want %d", eventID, count, err, want)
	}
}

func waitForExactJoinState(
	t *testing.T,
	ctx context.Context,
	coordinator *runtimepipeline.PipelineCoordinator,
	route runtimeflowidentity.Route,
	want string,
) runtimepipeline.WorkflowInstance {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		instance, found, err := coordinator.Load(ctx, testRunScopedWorkflowInstanceForRun(runtimecorrelation.RunIDFromContext(ctx), route.InstancePath))
		if err == nil && found && instance.CurrentState == want {
			return instance
		}
		time.Sleep(10 * time.Millisecond)
	}
	instance, found, err := coordinator.Load(ctx, testRunScopedWorkflowInstanceForRun(runtimecorrelation.RunIDFromContext(ctx), route.InstancePath))
	t.Fatalf("workflow did not reach %q = found:%v state:%q err:%v", want, found, instance.CurrentState, err)
	return runtimepipeline.WorkflowInstance{}
}

func exactJoinOccurrenceEventID(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, runID, eventName string) string {
	t.Helper()
	query := `SELECT event_id FROM events WHERE run_id = ? AND event_name = ?`
	if selected.postgres {
		query = `SELECT event_id::text FROM events WHERE run_id = $1::uuid AND event_name = $2`
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var eventID string
		if err := selected.db.QueryRowContext(ctx, query, runID, eventName).Scan(&eventID); err == nil {
			return eventID
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("durable join occurrence event was not published")
	return ""
}
