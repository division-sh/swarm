package pipeline_test

import (
	"context"
	"encoding/json"
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
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
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
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/division-sh/swarm/internal/testutil/flowroutefixture"
	"github.com/google/uuid"
)

type exactExternalJoinSource struct {
	semanticview.Source
	flowID string
	plans  []runtimecontracts.WorkflowJoinPlan
}

func exactJoinFlowPath(flowID string) string {
	if flowID == "" {
		return "."
	}
	return flowID
}

func (s exactExternalJoinSource) WorkflowJoins() []runtimecontracts.WorkflowJoinPlan {
	return append([]runtimecontracts.WorkflowJoinPlan(nil), s.plans...)
}

type exactJoinScheduleLogger struct{ t *testing.T }

type exactJoinRuntimeLogger struct {
	mu       sync.Mutex
	details  []string
	failures []exactJoinFailureLog
}

type exactJoinFailureLog struct {
	action, eventID string
	failure         runtimefailures.Envelope
}

func (l *exactJoinRuntimeLogger) Log(_ context.Context, _ diaglog.Level, _, _, action string, eventID string, _ string, _ string, _ string, _ string, _ map[string]string, detail any, failure *runtimefailures.Envelope, _ int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.details = append(l.details, fmt.Sprint(detail))
	if failure != nil {
		l.failures = append(l.failures, exactJoinFailureLog{action: action, eventID: eventID, failure: *runtimefailures.CloneEnvelope(failure)})
	}
	return nil
}

func (l *exactJoinRuntimeLogger) failuresFor(action, eventID string) []runtimefailures.Envelope {
	l.mu.Lock()
	defer l.mu.Unlock()
	var failures []runtimefailures.Envelope
	for _, record := range l.failures {
		if record.action == action && record.eventID == eventID {
			failures = append(failures, *runtimefailures.CloneEnvelope(&record.failure))
		}
	}
	return failures
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

// Hold only wakeup registration so committed state can be read before the real
// scheduler and EventBus execute the persisted continuation.
type exactJoinScheduleDriver struct {
	runtimegenericschedule.Scheduler
	mu      sync.Mutex
	started bool
	pending map[runtimegenericschedule.Wakeup]struct{}
}

func (d *exactJoinScheduleDriver) RegisterGenericScheduleWakeup(ctx context.Context, wakeup runtimegenericschedule.Wakeup) error {
	if err := wakeup.Validate(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.started {
		d.pending[wakeup] = struct{}{}
		return nil
	}
	return d.Scheduler.RegisterGenericScheduleWakeup(ctx, wakeup)
}

func (d *exactJoinScheduleDriver) RetireGenericScheduleWakeup(wakeup runtimegenericschedule.Wakeup) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.pending, wakeup)
	return d.Scheduler.RetireGenericScheduleWakeup(wakeup)
}

func (d *exactJoinScheduleDriver) Resume(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for wakeup := range d.pending {
		if err := d.Scheduler.RegisterGenericScheduleWakeup(ctx, wakeup); err != nil {
			return err
		}
		delete(d.pending, wakeup)
	}
	d.started = true
	return nil
}

func newExactJoinScheduleLifecycleForTest(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, eventBus *runtimebus.EventBus) (*runtimegenericschedule.Lifecycle, *exactJoinScheduleDriver) {
	t.Helper()
	store, ok := selected.events.(runtimegenericschedule.Store)
	if !ok {
		t.Fatalf("selected store %T lacks generic schedule ownership", selected.events)
	}
	workOwner, ok := worklifetime.OccurrenceFromContext(ctx)
	if !ok {
		t.Fatal("join occurrence proof context lacks work owner")
	}
	driver := &exactJoinScheduleDriver{
		Scheduler: runtimepipeline.NewSchedulerWithWorkOwner(workOwner),
		pending:   make(map[runtimegenericschedule.Wakeup]struct{}),
	}
	lifecycle, err := runtimegenericschedule.NewLifecycle(
		store, driver, eventBus, eventBus.EngineDispatcher(), exactJoinScheduleLogger{t: t}, executionposture.Live,
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
	return lifecycle, driver
}

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
				runtimeLogger := &exactJoinRuntimeLogger{}
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
				source := exactExternalWorkflowJoinSource(t, flowID)
				joinNode := externalPipelineSourceNode(t, source, flowID, "join-node")
				path := runID
				workflowName := semanticview.RootExecutionFlowID(source)
				instanceID := runID
				if flowID != "" {
					instanceID = uuid.NewString()
					path = flowID + "/" + instanceID
					workflowName = flowID
				}
				eventType := "item.completed"
				subscriptionType := source.WorkflowName() + "/item.completed"
				if flowID != "" {
					eventType = path + "/item.completed"
					subscriptionType = eventType
				}
				if owners := source.RuntimeEventOwners("item.completed"); len(owners) != 1 || !owners[0].Equal(joinNode) {
					t.Fatalf("join event owners = %#v", owners)
				}
				module := proposedEffectProofModule{
					source: source,
					nodes: []runtimepipeline.WorkflowNode{{
						Node: joinNode, Subscriptions: []events.EventType{events.EventType(subscriptionType)},
						ExecutionType: runtimecontracts.SystemNodeExecutionType,
					}},
				}
				probe := runtimelifecycleprobe.New()
				eventBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
					ContractBundle: source, TestLifecycleProbe: probe, Logger: runtimeLogger,
				}, "platform.join_complete", "platform.join_timeout")
				if err != nil {
					t.Fatalf("new join EventBus: %v", err)
				}
				lifecycle, driver := newExactJoinScheduleLifecycleForTest(t, ctx, selected, eventBus)
				coordinator := newGateRecoveryCoordinator(eventBus, selected, runtimepipeline.PipelineCoordinatorOptions{
					Module: module, Persistence: selected.persistence,
					GenericSchedules: lifecycle, TestLifecycleProbe: probe,
				})
				eventBus.SetInterceptors(coordinator)

				route := testRunScopedWorkflowInstanceForRun(runID, path).Route
				entityID := runtimeflowidentity.EntityID(path)
				createdAt := time.Now().UTC()
				{
					construction142Ctx := ctx
					construction142At := createdAt
					construction142Instance, construction142Lifecycle, err := coordinator.PrepareInitialEntryLifecycle(construction142Ctx, testRunScopedWorkflowInstanceForRun(runID, path), runtimepipeline.WorkflowInstance{
						InstanceID: instanceID, StorageRef: path, WorkflowName: workflowName, WorkflowVersion: source.WorkflowVersion(),
						EntityID: entityID, CurrentState: "awaiting", StageDefined: true, EnteredStageAt: createdAt, CreatedAt: createdAt,
						Fields:     map[string]any{"expected": []any{"a", "b"}},
						EntityType: "join_state",
					}, construction142At)
					if err != nil {
						t.Fatalf("prepare fixture initial lifecycle: %v", err)
					}
					construction142Command, err := flowactivationfixture.Command(construction142Ctx, construction142Instance, construction142Lifecycle, construction142At)
					if err != nil {
						t.Fatalf("prepare fixture activation command: %v", err)
					}
					construction142Committed, err := any(selected.events).(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(construction142Ctx, construction142Command)
					if err != nil {
						t.Fatalf("materialize exact join owner: %v", err)
					}
					if err == nil && !construction142Committed.Acknowledged {
						t.Fatal("fixture activation was not acknowledged")
					}
					if construction142Committed.Acknowledged && construction142Committed.Created {
						if finalizeErr := coordinator.FinalizeInitialEntryLifecycle(construction142Ctx, construction142Committed.Lifecycle); finalizeErr != nil {
							t.Fatalf("finalize fixture initial lifecycle: %v", finalizeErr)
						}
					}
				}
				if flowID != "" {
					if err := flowroutefixture.Publish(eventBus, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: testRunScopedWorkflowInstanceForRun(runID, route.InstancePath)}); err != nil {
						t.Fatalf("add flow join route: %v", err)
					}
				}
				initialInstance := waitForExactJoinState(t, ctx, coordinator, route, "awaiting")
				initialArm := exactJoinPersistedArm(t, initialInstance)
				deadlineSchedule := exactJoinPendingSchedule(t, selected, ctx, initialArm)

				eventsByMember := make([]events.Event, 0, 2)
				for _, member := range []string{"a", "b"} {
					producer := eventtest.RootRoutingSource(runID)
					wantTargetFlow := flowID
					envelope := events.EnvelopeForFlowInstance(
						events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), workflowName,
					)
					if flowID == "" {
						wantTargetFlow = source.WorkflowName()
					} else {
						producer = eventtest.ConcreteTemplateRoutingSource(flowID, path, entityID)
						envelope = events.EnvelopeForTargetRoute(
							events.EnvelopeForFlowInstance(envelope, path),
							events.RouteIdentity{FlowID: flowID, FlowInstance: path, EntityID: entityID},
						)
					}
					event := eventtest.ExistingRunRootIngressWithRoutingSource(
						uuid.NewString(), events.EventType(eventType), "operator", "",
						[]byte(`{"member_id":"`+member+`","result":{"value":"`+member+`"}}`),
						0, runID, envelope, producer, time.Now().UTC(),
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
					reader := selected.events.(runtimebus.PreparedPublishEventReader)
					prepared, found, readErr := reader.LoadPreparedPublishEvent(ctx, event.ID())
					missingReceipt := readErr != nil || !found || len(prepared.DeliveryRoutes) != 1 || len(prepared.DeliveryRoutes[0].Context.Joins) != 1
					if missingReceipt {
						resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(source, joinNode, eventType)
						t.Logf("member %s lacks committed exact join receipt: routes=%#v found=%v error=%v handler_match=%v handler_key=%q", member, prepared.DeliveryRoutes, found, readErr, resolved.Matched, resolved.HandlerEventKey)
					}
					handlerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					handlerCompletion, completedCursor, handlerErr := probe.WaitAfter(handlerCtx, runtimelifecycleprobe.Cursor{}, runtimelifecycleprobe.Signal{
						Kind: runtimelifecycleprobe.HandlerCompleted, EventID: event.ID(), SubscriberType: "node", SubscriberID: joinNode.Key(),
					})
					cancel()
					if handlerErr != nil || handlerCompletion.Status != "completed" {
						settleCtx, settleCancel := context.WithTimeout(ctx, 5*time.Second)
						settlement, _, settleErr := probe.WaitAfter(settleCtx, completedCursor, runtimelifecycleprobe.Signal{
							Kind: runtimelifecycleprobe.DeliveryStatusChanged, EventID: event.ID(), SubscriberType: "node", SubscriberID: joinNode.Key(),
						})
						quietErr := eventBus.WaitForQuiescence(settleCtx)
						settleCancel()
						t.Logf("member %s handler = %#v err=%v settlement=%#v settle_err=%v quiet_err=%v logs=%s", member, handlerCompletion, handlerErr, settlement, settleErr, quietErr, runtimeLogger.String())
						assertExactJoinDeliveryStatus(t, selected, ctx, event.ID(), joinNode.Key(), "delivered")
						t.Fatalf("wait member %s handler = %#v err=%v logs=%s", member, handlerCompletion, handlerErr, runtimeLogger.String())
					}
					assertExactJoinDeliveryStatus(t, selected, ctx, event.ID(), joinNode.Key(), "delivered")
					waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
						cancel()
						t.Fatalf("wait member %s quiescence: %v", member, err)
					}
					cancel()
					if missingReceipt {
						t.Fatalf("member %s executed without its committed exact join receipt", member)
					}
					receipt := prepared.DeliveryRoutes[0].Context.Joins[0]
					if receipt.Disposition != events.JoinAdmissionBound || !receipt.Ref.Equal(initialArm.JoinRef()) {
						t.Fatalf("member %s receipt = %#v, want bound exact entry %#v", member, receipt, initialArm.JoinRef())
					}
					eventsByMember = append(eventsByMember, event)
				}

				pendingInstance := waitForExactJoinState(t, ctx, coordinator, route, "awaiting")
				pendingArm := exactJoinPersistedArm(t, pendingInstance)
				if pendingArm.Status != joinruntime.StatusClosed || pendingArm.CloseReason != joinruntime.CloseReasonComplete ||
					!pendingArm.OutcomePending || pendingArm.OutcomeFired || pendingArm.Completed() != 2 ||
					!pendingArm.JoinRef().Equal(initialArm.JoinRef()) || len(pendingInstance.TransitionHistory) != 0 {
					t.Fatalf("committed completion before scheduler release = %#v, transitions=%#v", pendingArm, pendingInstance.TransitionHistory)
				}
				completionSchedule := exactJoinPendingSchedule(t, selected, ctx, pendingArm)
				retiredDeadline, found, err := selected.events.(runtimegenericschedule.Store).LoadGenericScheduleActivation(ctx, deadlineSchedule.ID)
				if err != nil || !found || retiredDeadline.Status != runtimegenericschedule.StatusCancelled {
					t.Fatalf("completion did not cancel its declared deadline: %#v found=%v err=%v", retiredDeadline, found, err)
				}
				if err := driver.Resume(ctx); err != nil {
					t.Fatalf("resume actual join schedule driver: %v", err)
				}
				completionID := exactJoinOccurrenceEventID(t, selected, ctx, runID, "platform.join_complete")
				handlerCtx, cancelHandler := context.WithTimeout(ctx, 5*time.Second)
				handlerCompletion, handlerErr := probe.WaitForHandlerCompleted(handlerCtx, completionID, joinNode.Key())
				cancelHandler()
				if handlerErr != nil || handlerCompletion.Status != "completed" {
					t.Fatalf("wait exact join completion handler = %#v err=%v", handlerCompletion, handlerErr)
				}
				assertExactJoinDeliveryStatus(t, selected, ctx, completionID, joinNode.Key(), "delivered")
				instance := waitForExactJoinState(t, ctx, coordinator, route, "ready")
				carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.Gates, instance.StateBuckets)
				if err != nil {
					t.Fatal(err)
				}
				joins, err := joinruntime.List(carrier.StateBuckets)
				if err != nil || len(joins) != 1 {
					t.Fatalf("join readback = %#v err=%v", joins, err)
				}
				if joins[0].Status != joinruntime.StatusClosed || joins[0].CloseReason != joinruntime.CloseReasonComplete ||
					joins[0].FlowPath() != exactJoinFlowPath(flowID) || joins[0].Completed() != 2 || !joins[0].TimerCancelled ||
					!joins[0].OutcomeFired || joins[0].OutcomePending || !joins[0].JoinRef().Equal(pendingArm.JoinRef()) || len(instance.TransitionHistory) != 1 {
					t.Fatalf("closed join = %#v", joins[0])
				}
				assertExactJoinFiredSchedule(t, selected, ctx, completionSchedule, completionID)

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
				assertPersistedHandlerRuleSelectionInFlow(
					t, selected, ctx, completionID, handlerselection.ContextJoinComplete,
					handlerselection.DispositionSelected, exactJoinFlowPath(flowID), `nodes["join-node"].handlers["item.completed"].join.on_complete[0]`, "",
				)
				assertTraceHandlerRuleSelectionInFlow(
					t, selected, ctx, runID, completionID, handlerselection.ContextJoinComplete,
					handlerselection.DispositionSelected, exactJoinFlowPath(flowID), `nodes["join-node"].handlers["item.completed"].join.on_complete[0]`, "",
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
		semanticPath  string
	}{
		{name: "completion", expected: []any{}, timeout: "1h", eventName: "platform.join_complete", terminalState: "ready", closeReason: joinruntime.CloseReasonComplete, context: handlerselection.ContextJoinComplete, semanticPath: `nodes["join-node"].handlers["item.completed"].join.on_complete[0]`},
		{name: "deadline", expected: []any{"a"}, timeout: "20ms", eventName: "platform.join_timeout", terminalState: "attention", closeReason: joinruntime.CloseReasonDeadline, context: handlerselection.ContextJoinTimeout, semanticPath: `nodes["join-node"].handlers["item.completed"].join.on_deadline[0]`},
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
					workflowName := semanticview.RootExecutionFlowID(source)
					instanceID := runID
					if flowID != "" {
						instanceID = uuid.NewString()
						path = flowID + "/" + instanceID
						workflowName = flowID
					}

					module := proposedEffectProofModule{
						source: source,
						nodes: []runtimepipeline.WorkflowNode{{
							Node: joinNode, ExecutionType: runtimecontracts.SystemNodeExecutionType,
							Subscriptions: []events.EventType{"item.completed"},
						}},
					}
					probe := runtimelifecycleprobe.New()
					eventBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
						ContractBundle: source, TestLifecycleProbe: probe, Logger: runtimeLogger,
					}, outcome.eventName)
					if err != nil {
						t.Fatalf("new schedule occurrence EventBus: %v", err)
					}
					lifecycle, driver := newExactJoinScheduleLifecycleForTest(t, ctx, selected, eventBus)
					coordinator := newGateRecoveryCoordinator(eventBus, selected, runtimepipeline.PipelineCoordinatorOptions{
						Module: module, Persistence: selected.persistence,
						GenericSchedules: lifecycle, TestLifecycleProbe: probe,
					})
					eventBus.SetInterceptors(coordinator)

					route := testRunScopedWorkflowInstanceForRun(runID, path).Route
					entityID := runtimeflowidentity.EntityID(path)
					createdAt := time.Now().UTC()
					{
						construction337Ctx := ctx
						construction337At := createdAt
						construction337Instance, construction337Lifecycle, err := coordinator.PrepareInitialEntryLifecycle(construction337Ctx, testRunScopedWorkflowInstanceForRun(runID, path), runtimepipeline.WorkflowInstance{
							InstanceID: instanceID, StorageRef: path, WorkflowName: workflowName, WorkflowVersion: source.WorkflowVersion(),
							EntityID: entityID, CurrentState: "awaiting", StageDefined: true, EnteredStageAt: createdAt, CreatedAt: createdAt,
							Fields:     map[string]any{"expected": outcome.expected},
							EntityType: "join_state",
						}, construction337At)
						if err != nil {
							t.Fatalf("prepare fixture initial lifecycle: %v", err)
						}
						construction337Command, err := flowactivationfixture.Command(construction337Ctx, construction337Instance, construction337Lifecycle, construction337At)
						if err != nil {
							t.Fatalf("prepare fixture activation command: %v", err)
						}
						construction337Committed, err := any(selected.events).(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(construction337Ctx, construction337Command)
						if err != nil {
							t.Fatalf("materialize immediate join owner: %v", err)
						}
						if err == nil && !construction337Committed.Acknowledged {
							t.Fatal("fixture activation was not acknowledged")
						}
						if construction337Committed.Acknowledged && construction337Committed.Created {
							if finalizeErr := coordinator.FinalizeInitialEntryLifecycle(construction337Ctx, construction337Committed.Lifecycle); finalizeErr != nil {
								t.Fatalf("finalize fixture initial lifecycle: %v", finalizeErr)
							}
						}
					}
					if flowID != "" {
						if err := flowroutefixture.Publish(eventBus, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: testRunScopedWorkflowInstanceForRun(runID, route.InstancePath)}); err != nil {
							t.Fatalf("add flow join route: %v", err)
						}
					}

					initialInstance := waitForExactJoinState(t, ctx, coordinator, route, "awaiting")
					initialArm := exactJoinPersistedArm(t, initialInstance)
					if initialArm.OutcomeFired || len(initialInstance.TransitionHistory) != 0 ||
						(outcome.closeReason == joinruntime.CloseReasonComplete && (initialArm.Status != joinruntime.StatusClosed || !initialArm.OutcomePending || initialArm.CloseReason != joinruntime.CloseReasonComplete)) ||
						(outcome.closeReason == joinruntime.CloseReasonDeadline && (initialArm.Status != joinruntime.StatusOpen || initialArm.OutcomePending)) {
						t.Fatalf("committed arm before scheduler release = %#v, transitions=%#v", initialArm, initialInstance.TransitionHistory)
					}
					pendingSchedule := exactJoinPendingSchedule(t, selected, ctx, initialArm)
					if err := driver.Resume(ctx); err != nil {
						t.Fatalf("resume actual join schedule driver: %v", err)
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
					if joins[0].Status != joinruntime.StatusClosed || joins[0].FlowPath() != exactJoinFlowPath(flowID) ||
						joins[0].CloseReason != outcome.closeReason || !joins[0].OutcomeFired || joins[0].OutcomePending ||
						!joins[0].JoinRef().Equal(initialArm.JoinRef()) {
						t.Fatalf("fired join occurrence = %#v", joins[0])
					}
					assertExactJoinFiredSchedule(t, selected, ctx, pendingSchedule, eventID)
					assertPersistedHandlerRuleSelectionInFlow(
						t, selected, ctx, eventID, outcome.context, handlerselection.DispositionSelected,
						exactJoinFlowPath(flowID), outcome.semanticPath, "",
					)
					assertTraceHandlerRuleSelectionInFlow(
						t, selected, ctx, runID, eventID, outcome.context, handlerselection.DispositionSelected,
						exactJoinFlowPath(flowID), outcome.semanticPath, "",
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

func exactJoinPersistedArm(t *testing.T, instance runtimepipeline.WorkflowInstance) joinruntime.Activation {
	t.Helper()
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.Gates, instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	arms, err := joinruntime.List(carrier.StateBuckets)
	if err != nil || len(arms) != 1 {
		t.Fatalf("exact persisted join arm = %#v err=%v", arms, err)
	}
	owner := testRunScopedWorkflowInstanceForRun(arms[0].JoinRef().StageEntry().RunID, instance.StorageRef)
	if err := arms[0].JoinRef().StageEntry().RequireOwner(
		owner.RunID, owner.Route.ScopeKey,
		instance.InstanceID, instance.StorageRef, instance.EntityID, "awaiting",
	); err != nil {
		t.Fatalf("persisted join entry contradicts its concrete owner: %v", err)
	}
	return arms[0]
}

func exactJoinPendingSchedule(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, arm joinruntime.Activation) runtimegenericschedule.Activation {
	t.Helper()
	activations, err := selected.events.(runtimegenericschedule.Store).ListActiveGenericScheduleActivations(ctx)
	if err != nil {
		t.Fatalf("read committed join schedules: %v", err)
	}
	var matches []runtimegenericschedule.Activation
	for _, activation := range activations {
		if activation.Command.RunID == runtimecorrelation.RunIDFromContext(ctx) && activation.Command.TaskID == arm.TimerTaskID() {
			matches = append(matches, activation)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("committed exact join schedule = %#v, want one for %q", matches, arm.TimerTaskID())
	}
	activation := matches[0]
	if err := activation.Validate(); err != nil {
		t.Fatalf("invalid committed join schedule: %v", err)
	}
	payload, ok := activation.Command.Payload.Interface().(map[string]any)
	handle, ref, valid := timeridentity.ParseJoinHandle(payload)
	entry := arm.JoinRef().StageEntry()
	scheduleFlowInstance := entry.InstancePath
	if arm.FlowPath() == "." {
		scheduleFlowInstance = ""
	}
	if !ok || !valid || handle != arm.TimerHandle() || !ref.Equal(arm.JoinRef()) ||
		activation.Command.EventType != handle.EventType() || activation.Command.EntityID != entry.EntityID ||
		entry.RunID != runtimecorrelation.RunIDFromContext(ctx) || activation.Command.FlowInstance != scheduleFlowInstance || activation.CurrentEventID != "" ||
		!activation.FiredAt.IsZero() || !activation.AcceptedAt.IsZero() {
		t.Fatalf("pending schedule lacks exact declared handle/entry: %#v, arm=%#v", activation, arm)
	}
	return activation
}

func assertExactJoinFiredSchedule(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, pending runtimegenericschedule.Activation, eventID string) {
	t.Helper()
	store := selected.events.(runtimegenericschedule.Store)
	fired, found, err := store.LoadGenericScheduleActivation(ctx, pending.ID)
	if err != nil || !found || fired.Status != runtimegenericschedule.StatusFired || fired.CurrentEventID != eventID ||
		fired.ImmutableHash != pending.ImmutableHash || !fired.InitialDueAt.Equal(pending.InitialDueAt) ||
		fired.FiredAt.IsZero() || fired.AcceptedAt.IsZero() {
		t.Fatalf("exact schedule was not durably fired: %#v found=%v err=%v, pending=%#v event=%s", fired, found, err, pending, eventID)
	}
	if err := fired.Validate(); err != nil {
		t.Fatalf("invalid fired join schedule: %v", err)
	}
	prepared, found, err := selected.events.(runtimebus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, eventID)
	if err != nil || !found {
		t.Fatalf("read committed join occurrence: found=%v err=%v", found, err)
	}
	event := prepared.Event.Event()
	var payload map[string]any
	if err := json.Unmarshal(event.Payload(), &payload); err != nil {
		t.Fatal(err)
	}
	handle, ref, valid := timeridentity.ParseJoinHandle(payload)
	schedulePayload, ok := pending.Command.Payload.Interface().(map[string]any)
	expectedHandle, expectedRef, expectedValid := timeridentity.ParseJoinHandle(schedulePayload)
	if !valid || !ok || !expectedValid || handle != expectedHandle || !ref.Equal(expectedRef) ||
		event.TaskID() != pending.Command.TaskID || string(event.Type()) != pending.Command.EventType ||
		event.Producer().ID() != runtimegenericschedule.OccurrenceProducerID() || len(prepared.DeliveryRoutes) != 1 ||
		prepared.DeliveryRoutes[0].Recipient.ID() != ref.Node().Key() ||
		prepared.DeliveryRoutes[0].Target.Route().FlowInstance != ref.StageEntry().InstancePath ||
		prepared.DeliveryRoutes[0].Target.Route().EntityID != ref.StageEntry().EntityID {
		t.Fatalf("published occurrence contradicts its exact committed schedule: event=%#v routes=%#v", event, prepared.DeliveryRoutes)
	}
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
