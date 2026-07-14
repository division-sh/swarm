package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/google/uuid"
)

type pipelineEmitCollectorKey struct{}
type pipelineEmitIntentCollectorKey struct{}
type pipelineSourceAgentKey struct{}

func withPipelineSourceAgent(ctx context.Context, sourceAgent string) context.Context {
	sourceAgent = strings.TrimSpace(sourceAgent)
	if sourceAgent == "" {
		return ctx
	}
	return context.WithValue(ctx, pipelineSourceAgentKey{}, sourceAgent)
}

func pipelineSourceAgent(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(pipelineSourceAgentKey{}).(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

type PipelineCoordinator struct {
	bus Bus
	db  *sql.DB

	mu sync.Mutex

	entityLockMu sync.Mutex
	entityLocks  map[string]*sync.Mutex

	module                  WorkflowModule
	workflowStore           *WorkflowInstanceStore
	expressionEval          *workflowExpressionEvaluator
	instanceActivator       FlowInstanceActivator
	instanceDeactivator     FlowInstanceDeactivator
	timerScheduler          *Scheduler
	timerScheduleStore      SchedulePersistence
	mailboxMaterializer     MailboxWriteMaterializationStore
	decisionCards           decisioncard.Store
	eventReceiptsCapability func(context.Context) (bool, error)
	credentials             runtimecredentials.Store
	managedCredentials      runtimemanagedcredentials.Store
	artifactRoot            string
	bundleFingerprint       string
	decisionCardCadence     decisioncard.CadencePolicy

	testSubscribeHook                func()
	testEntityStateHook              func(entityID, state string)
	testWorkflowNodeHandlerStartHook WorkflowNodeHandlerStartHook
	testLifecycleProbe               runtimelifecycleprobe.Observer
	testEngineEmitNow                func() time.Time
}

type WorkflowNodeHandlerStartHook func(context.Context, string, events.Event) error

type PipelineCoordinatorOptions struct {
	ShardPlanner                     any
	Module                           WorkflowModule
	WorkflowStore                    *WorkflowInstanceStore
	InstanceActivator                FlowInstanceActivator
	InstanceDeactivator              FlowInstanceDeactivator
	TimerScheduler                   *Scheduler
	TimerScheduleStore               SchedulePersistence
	MailboxMaterializer              MailboxWriteMaterializationStore
	DecisionCards                    decisioncard.Store
	EventReceiptsCapability          func(context.Context) (bool, error)
	Credentials                      runtimecredentials.Store
	ManagedCredentials               runtimemanagedcredentials.Store
	ArtifactRoot                     string
	BundleFingerprint                string
	DecisionCardCadence              decisioncard.CadencePolicy
	TestEntityStateHook              func(entityID, state string)
	TestWorkflowNodeHandlerStartHook WorkflowNodeHandlerStartHook
	TestLifecycleProbe               runtimelifecycleprobe.Observer
	TestEngineEmitNow                func() time.Time
}

func NewPipelineCoordinatorWithOptions(bus Bus, db *sql.DB, opts PipelineCoordinatorOptions) *PipelineCoordinator {
	if bus == nil {
		return nil
	}
	module := opts.Module
	if module == nil {
		panic("pipeline: workflow module is required")
	}
	workflowStore := opts.WorkflowStore
	if workflowStore == nil {
		workflowStore = NewWorkflowInstanceStore(db)
	}
	if publisher, ok := bus.(workflowGateMutationPublisher); ok {
		workflowStore.ConfigureDecisionCardLifecycle(opts.DecisionCards, publisher)
	} else {
		workflowStore.ConfigureDecisionCardLifecycle(opts.DecisionCards)
	}
	credentials := opts.Credentials
	if credentials == nil {
		credentials = runtimecredentials.NewEnvStore()
	}
	return &PipelineCoordinator{
		bus:                              bus,
		db:                               db,
		module:                           module,
		workflowStore:                    workflowStore,
		expressionEval:                   newWorkflowExpressionEvaluator(),
		instanceActivator:                opts.InstanceActivator,
		instanceDeactivator:              opts.InstanceDeactivator,
		timerScheduler:                   opts.TimerScheduler,
		timerScheduleStore:               opts.TimerScheduleStore,
		mailboxMaterializer:              opts.MailboxMaterializer,
		decisionCards:                    opts.DecisionCards,
		eventReceiptsCapability:          opts.EventReceiptsCapability,
		credentials:                      credentials,
		managedCredentials:               opts.ManagedCredentials,
		artifactRoot:                     strings.TrimSpace(opts.ArtifactRoot),
		bundleFingerprint:                strings.TrimSpace(opts.BundleFingerprint),
		decisionCardCadence:              opts.DecisionCardCadence.Normalize(),
		testEntityStateHook:              opts.TestEntityStateHook,
		testWorkflowNodeHandlerStartHook: opts.TestWorkflowNodeHandlerStartHook,
		testLifecycleProbe:               opts.TestLifecycleProbe,
		testEngineEmitNow:                opts.TestEngineEmitNow,
		entityLocks:                      make(map[string]*sync.Mutex),
	}
}

func NewPipelineCoordinator(bus Bus, db *sql.DB) *PipelineCoordinator {
	panic("pipeline: workflow module is required")
}

func (pc *PipelineCoordinator) SetTestSubscribeHook(fn func()) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.testSubscribeHook = fn
	pc.mu.Unlock()
}

func (pc *PipelineCoordinator) SetTestEntityStateHook(fn func(entityID, state string)) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.testEntityStateHook = fn
	pc.mu.Unlock()
}

func (pc *PipelineCoordinator) SetTestWorkflowNodeHandlerStartHook(fn WorkflowNodeHandlerStartHook) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.testWorkflowNodeHandlerStartHook = fn
	pc.mu.Unlock()
}

func (pc *PipelineCoordinator) SetTestLifecycleProbe(probe runtimelifecycleprobe.Observer) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.testLifecycleProbe = probe
	pc.mu.Unlock()
}

func (pc *PipelineCoordinator) Run(ctx context.Context) {
	if pc == nil || pc.bus == nil {
		return
	}
	ch := pc.subscribe()
	pc.notifyTestSubscribed()
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				ch = pc.subscribe()
				pc.notifyTestSubscribed()
				continue
			}
			if _, err := pc.handleEventResult(ctx, evt); err != nil && pc.bus != nil {
				pc.bus.LogRuntime(ctx, RuntimeLogEntry{
					Level:     "error",
					Message:   "Workflow handler execution failed",
					Component: runtimeWorkflowID,
					Action:    "handler_error",
					EventID:   strings.TrimSpace(evt.ID()),
					EventType: strings.TrimSpace(string(evt.Type())),
					EntityID:  workflowEventEntityID(evt),
					Failure:   pipelineRuntimeFailure(err, runtimeWorkflowID, "handle_event"),
				})
			}
		}
	}
}

func (pc *PipelineCoordinator) RunMaintenance(ctx context.Context) {
	draftExpiry, hasDraftExpiry := pc.decisionCards.(interface {
		ExpireDecisionCardInputDrafts(context.Context, time.Time) (int, error)
	})
	humanTaskExpiry, hasHumanTaskExpiry := pc.decisionCards.(interface {
		ExpireHumanTaskCards(context.Context, time.Time, int) (int, error)
	})
	if (!hasDraftExpiry || draftExpiry == nil) && (!hasHumanTaskExpiry || humanTaskExpiry == nil) {
		return
	}
	run := func() {
		now := time.Now().UTC()
		if hasDraftExpiry && draftExpiry != nil {
			if _, err := draftExpiry.ExpireDecisionCardInputDrafts(ctx, now); err != nil {
				pc.logRuntimeWarn(ctx, runtimeWorkflowID, "expire_decision_card_input_drafts", "", "", runtimeWorkflowID, "", nil, err)
			}
		}
		if hasHumanTaskExpiry && humanTaskExpiry != nil {
			if _, err := humanTaskExpiry.ExpireHumanTaskCards(ctx, now, 200); err != nil {
				pc.logRuntimeWarn(ctx, runtimeWorkflowID, "expire_human_task_cards", "", "", runtimeWorkflowID, "", nil, err)
			}
		}
	}
	run()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func (pc *PipelineCoordinator) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	if pc == nil {
		return true, nil, nil
	}
	eventType := strings.TrimSpace(string(evt.Type()))
	if eventType == "" {
		return true, nil, nil
	}
	if evt.Type() == workflowGateDecisionEventType {
		emitted, err := pc.handleWorkflowGateDecisionEvent(ctx, evt)
		return false, emitted, err
	}
	if evt.Type() == decisionCardDeferredEventType {
		emitted, err := pc.handleDecisionCardDeferredEvent(ctx, evt)
		return false, emitted, err
	}
	if evt.Type() == decisionCardExpiredEventType {
		emitted, err := pc.handleDecisionCardExpiredEvent(ctx, evt)
		return false, emitted, err
	}
	stageTimer, firedStageTimer, err := pc.handleWorkflowStageTimerFire(ctx, evt)
	if err != nil {
		return false, nil, err
	}
	if stageTimer && (!firedStageTimer || eventType == runtimecontracts.WorkflowStageTimerInternalEvent) {
		return false, nil, nil
	}
	consume, handled := pc.interceptPolicy(ctx, eventType, evt)
	if !handled {
		return true, nil, nil
	}
	emitted := make([]events.Event, 0, 4)
	ictx := context.WithValue(ctx, pipelineEmitCollectorKey{}, &emitted)
	handled, err = pc.handleEventResult(ictx, evt)
	if evt.Type() == activityRequestEventType && err != nil {
		return false, emitted, err
	}
	if err != nil {
		if consume {
			return false, emitted, nil
		}
		return true, emitted, nil
	}
	if !handled {
		return true, nil, nil
	}
	return !consume, emitted, nil
}

func (pc *PipelineCoordinator) InterceptDeliveryRoute(ctx context.Context, evt events.Event, route events.DeliveryRoute) (bool, []events.Event, error) {
	route = route.Normalized()
	if route.SubscriberType != "node" || route.SubscriberID == "" {
		return true, nil, nil
	}
	if route.Target.Normalized() != evt.TargetRoute().Normalized() {
		return true, nil, fmt.Errorf("workflow node delivery route target mismatch for %s: route=%#v event=%#v", route.SubscriberID, route.Target.Normalized(), evt.TargetRoute().Normalized())
	}
	return pc.Intercept(withWorkflowNodeDeliveryRoute(ctx, route), evt)
}

func (pc *PipelineCoordinator) interceptPolicy(ctx context.Context, eventType string, evt events.Event) (consume bool, handled bool) {
	if strings.TrimSpace(eventType) == "" {
		return false, false
	}
	return pc.workflowNodeInterceptPolicy(ctx, eventType, evt)
}

func (pc *PipelineCoordinator) subscribe() <-chan events.Event {
	subscriptions := workflowRuntimeSubscriptions(pc.WorkflowNodes())
	if internalBus, ok := any(pc.bus).(interface {
		SubscribeInternal(string, ...events.EventType) <-chan events.Event
	}); ok {
		return internalBus.SubscribeInternal(runtimeWorkflowID, subscriptions...)
	}
	return pc.bus.Subscribe(runtimeWorkflowID, subscriptions...)
}

func (pc *PipelineCoordinator) handleEvent(ctx context.Context, evt events.Event) bool {
	handled, _ := pc.handleEventResult(ctx, evt)
	return handled
}

func (pc *PipelineCoordinator) handleEventResult(ctx context.Context, evt events.Event) (bool, error) {
	if evt.Type() == activityRequestEventType {
		return pc.handleActivityRequestEvent(ctx, evt)
	}
	return pc.dispatchWorkflowNodeEventResult(ctx, evt)
}

func (pc *PipelineCoordinator) executeNodeHandlerPlan(ctx context.Context, nodeID string, evt events.Event) bool {
	handled, _ := pc.executeNodeHandlerPlanResult(ctx, nodeID, evt)
	return handled
}

func (pc *PipelineCoordinator) executeNodeHandlerPlanResult(ctx context.Context, nodeID string, evt events.Event) (bool, error) {
	if pc == nil {
		return false, nil
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return false, nil
	}
	source := pc.SemanticSource()
	if source == nil {
		return false, nil
	}
	trigger := strings.TrimSpace(string(evt.Type()))
	if trigger == "" {
		return false, nil
	}
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, nodeID, evt)
	handler := resolved.Handler
	handlerEventKey := resolved.HandlerEventKey
	ok := resolved.Matched
	if !ok && isJoinLifecycleEvent(events.EventType(trigger)) {
		if ref, _, refOK := timeridentity.ParseJoinRef(parsePayloadMap(evt.Payload())); refOK && ref.NodeID == nodeID {
			handler, ok = findJoinHandlerForRef(source, ref)
			handlerEventKey = ref.HandlerEvent
		}
	}
	if !ok {
		return false, nil
	}
	if pc.workflowNodeEventProcessed(ctx, nodeID, evt) {
		return true, nil
	}
	if !pc.workflowNodeDeliveryAuthorized(ctx, nodeID, evt) {
		return false, nil
	}
	if !pc.markWorkflowNodeDeliveryInProgress(ctx, nodeID, evt) {
		return false, nil
	}
	ctx = withPipelineFlowScope(ctx, workflowNodeFlowID(source, nodeID))
	if err := pc.notifyTestWorkflowNodeHandlerStarting(ctx, nodeID, evt); err != nil {
		return false, err
	}
	pc.notifyTestLifecycleHandlerStarted(ctx, nodeID, evt)
	result, err := pc.executeNodeContractHandler(ctx, nodeID, handler, workflowTriggerContext{
		Event:           evt,
		HandlerEventKey: handlerEventKey,
		State:           pc.currentWorkflowState(ctx, workflowEventEntityID(evt)),
	}, false)
	if err != nil {
		pc.notifyTestLifecycleHandlerCompleted(ctx, nodeID, evt, "failed")
		failure := runtimefailures.FromError(err, runtimeWorkflowID, "execute_handler")
		if errors.Is(err, runtimeengine.ErrChainDepthExceeded) {
			_ = recordPipelineDeadLetter(ctx, pc.db, runtimedeadletters.Record{
				OriginalEventID: strings.TrimSpace(evt.ID()),
				Failure:         failure.Failure,
				ChainDepth:      evt.ChainDepth(),
				HandlerNode:     nodeID,
			})
			setPipelineReceiptOverride(ctx, "dead_letter", &failure.Failure)
			pc.markWorkflowNodeDeliveryDeadLetter(ctx, nodeID, evt, "chain_depth_exceeded", &failure.Failure, 0)
			return true, nil
		}
		pc.recordWorkflowHandlerFailure(ctx, evt, nodeID, err)
		pc.markWorkflowNodeDeliveryDeadLetter(ctx, nodeID, evt, "handler_terminal_failure", &failure.Failure, 0)
		return true, err
	}
	pc.notifyTestLifecycleHandlerCompleted(ctx, nodeID, evt, "completed")
	pc.recordInterceptedEmitDeadLetters(ctx, evt, nodeID, result.Outcome)
	if result.Handled {
		pc.reconcileWorkflowEventTimers(ctx, workflowEventEntityID(evt), trigger)
	}
	return result.Handled, nil
}

func (pc *PipelineCoordinator) recordWorkflowHandlerFailure(ctx context.Context, evt events.Event, nodeID string, err error) {
	if pc == nil || err == nil {
		return
	}
	failure := runtimefailures.FromError(err, runtimeWorkflowID, "execute_handler")
	setPipelineReceiptOverride(ctx, "dead_letter", &failure.Failure)
	if pc.bus != nil {
		pc.bus.LogRuntime(ctx, RuntimeLogEntry{
			Level:     "error",
			Message:   "Workflow handler execution failed",
			Component: runtimeWorkflowID,
			Action:    "handler_error",
			EventID:   strings.TrimSpace(evt.ID()),
			EventType: strings.TrimSpace(string(evt.Type())),
			EntityID:  workflowEventEntityID(evt),
			Detail: map[string]any{
				"node_id": nodeID,
			},
			Failure: &failure.Failure,
		})
	}
	if pc.db != nil {
		_ = recordPipelineDeadLetter(ctx, pc.db, runtimedeadletters.Record{
			OriginalEventID: strings.TrimSpace(evt.ID()),
			OriginalEvent:   strings.TrimSpace(string(evt.Type())),
			OriginalPayload: evt.Payload(),
			EntityID:        workflowEventEntityID(evt),
			FlowInstance:    "runtime",
			Failure:         failure.Failure,
			RetryCount:      0,
			ChainDepth:      evt.ChainDepth(),
			HandlerNode:     strings.TrimSpace(nodeID),
		})
	}
}

func (pc *PipelineCoordinator) recordInterceptedEmitDeadLetters(ctx context.Context, trigger events.Event, nodeID string, outcome *handlerExecutionOutcome) {
	if pc == nil || outcome == nil || len(outcome.InterceptedEmits) == 0 {
		return
	}
	entityID := workflowEventEntityID(trigger)
	nodeID = strings.TrimSpace(nodeID)
	for _, intercepted := range outcome.InterceptedEmits {
		if strings.TrimSpace(intercepted.DeadLetterHint) != "chain_depth_exceeded" {
			continue
		}
		eventType := strings.TrimSpace(string(intercepted.Event.Type()))
		failure := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassChainDepthExceeded, "chain_depth_exceeded", runtimeWorkflowID, "emit", map[string]any{
			"event_type": eventType, "chain_depth": intercepted.ChainDepth,
		}), runtimeWorkflowID, "emit")
		rec := runtimedeadletters.Record{
			OriginalEventID: strings.TrimSpace(trigger.ID()),
			OriginalEvent:   eventType,
			OriginalPayload: intercepted.Event.Payload(),
			EntityID:        entityID,
			FlowInstance:    "runtime",
			Failure:         failure.Failure,
			ChainDepth:      intercepted.ChainDepth,
			HandlerNode:     firstNonEmptyString(nodeID+":"+eventType, nodeID),
		}
		if pc.db != nil {
			if err := recordPipelineDeadLetter(ctx, pc.db, rec); err != nil {
				pc.logRuntimeWarn(ctx, "workflow-runtime", "intercepted_emit_dead_letter_persist_failed", strings.TrimSpace(trigger.ID()), strings.TrimSpace(string(trigger.Type())), runtimeWorkflowID, entityID, map[string]any{
					"intercepted_event_type": eventType,
					"handler_node":           nodeID,
				}, err)
			}
		}
		deadLetterPayload := map[string]any{
			"original_event":   eventType,
			"original_payload": json.RawMessage(intercepted.Event.Payload()),
			"entity_id":        entityID,
			"flow_instance":    "runtime",
			"failure":          failure.Failure,
			"retry_count":      0,
			"chain_depth":      intercepted.ChainDepth,
			"handler_node":     nodeID,
			"timestamp":        time.Now().UTC().Format(time.RFC3339Nano),
		}
		if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
			sourceAgent := pipelineSourceAgent(ctx)
			if sourceAgent == "" {
				sourceAgent = runtimeWorkflowID
			}
			*collector = append(*collector, events.NewRuntimeDiagnosticEvent(
				uuid.NewString(),
				events.EventType("platform.dead_letter"),
				sourceAgent,
				"",
				mustJSON(deadLetterPayload),
				0,
				"",
				"",
				events.EventEnvelope{EntityID: entityID},
				time.Now().UTC(),
			))
			continue
		}
		publishDeadLetter := func() {
			if err := pc.publish(ctx, "platform.dead_letter", entityID, deadLetterPayload); err != nil {
				pc.logRuntimeWarn(ctx, "workflow-runtime", "intercepted_emit_dead_letter_publish_failed", strings.TrimSpace(trigger.ID()), strings.TrimSpace(string(trigger.Type())), runtimeWorkflowID, entityID, map[string]any{
					"intercepted_event_type": eventType,
					"handler_node":           nodeID,
				}, err)
			}
		}
		if !queuePipelinePostCommitAction(ctx, publishDeadLetter) {
			publishDeadLetter()
		}
	}
}

func (pc *PipelineCoordinator) logRuntimeWarn(ctx context.Context, component, action, eventID, eventType, agentID, entityID string, detail any, err error) {
	if pc != nil && pc.bus != nil {
		pc.bus.LogRuntime(ctx, RuntimeLogEntry{
			Level:     "warn",
			Message:   "Workflow runtime warning was recorded",
			Component: strings.TrimSpace(component),
			Action:    strings.TrimSpace(action),
			EventID:   strings.TrimSpace(eventID),
			EventType: strings.TrimSpace(eventType),
			AgentID:   strings.TrimSpace(agentID),
			EntityID:  strings.TrimSpace(entityID),
			Detail:    detail,
			Failure:   pipelineRuntimeFailure(err, strings.TrimSpace(component), strings.TrimSpace(action)),
		})
		return
	}
	processWarn(component, "%s", strings.TrimSpace(errText(err)))
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (pc *PipelineCoordinator) notifyTestSubscribed() {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	hook := pc.testSubscribeHook
	pc.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (pc *PipelineCoordinator) notifyTestEntityStateUpdated(entityID, state string) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	hook := pc.testEntityStateHook
	pc.mu.Unlock()
	if hook != nil {
		hook(strings.TrimSpace(entityID), strings.TrimSpace(state))
	}
}

func (pc *PipelineCoordinator) notifyTestWorkflowNodeHandlerStarting(ctx context.Context, nodeID string, evt events.Event) error {
	if pc == nil {
		return nil
	}
	pc.mu.Lock()
	hook := pc.testWorkflowNodeHandlerStartHook
	pc.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(ctx, strings.TrimSpace(nodeID), evt)
}

func (pc *PipelineCoordinator) publish(ctx context.Context, eventType, entityID string, payload map[string]any) error {
	if pc == nil {
		return nil
	}
	if payload == nil {
		payload = map[string]any{}
	}
	sourceAgent := pipelineSourceAgent(ctx)
	if sourceAgent == "" {
		sourceAgent = runtimeWorkflowID
	}
	flowInstance := strings.Trim(strings.TrimSpace(asString(payload["flow_instance"])), "/")
	emitted := events.NewRuntimeDiagnosticEvent(
		uuid.NewString(),
		events.EventType(strings.TrimSpace(eventType)),
		sourceAgent,
		"",
		mustJSON(payload),
		0,
		"",
		"",
		events.EventEnvelope{
			EntityID:     entityID,
			FlowInstance: flowInstance,
		},
		time.Now().UTC(),
	)
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		*collector = append(*collector, emitted)
		return nil
	}
	if pc.bus != nil {
		if err := pc.bus.Publish(ctx, emitted); err != nil {
			return err
		}
	}
	return nil
}

func (pc *PipelineCoordinator) publishDirect(ctx context.Context, eventType, entityID string, payload map[string]any, recipients []string) error {
	if pc == nil {
		return nil
	}
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return pc.publish(ctx, eventType, entityID, payload)
	}
	sourceAgent := pipelineSourceAgent(ctx)
	if sourceAgent == "" {
		sourceAgent = runtimeWorkflowID
	}
	flowInstance := strings.Trim(strings.TrimSpace(asString(payload["flow_instance"])), "/")
	emitted := events.NewRuntimeDiagnosticEvent(
		uuid.NewString(),
		events.EventType(strings.TrimSpace(eventType)),
		sourceAgent,
		"",
		mustJSON(payload),
		0,
		"",
		"",
		events.EventEnvelope{
			EntityID:     entityID,
			FlowInstance: flowInstance,
		},
		time.Now().UTC(),
	)
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		*collector = append(*collector, emitted)
		return nil
	}
	if pc.bus != nil {
		if err := pc.bus.PublishDirect(ctx, emitted, recipients); err != nil {
			return err
		}
	}
	return nil
}
