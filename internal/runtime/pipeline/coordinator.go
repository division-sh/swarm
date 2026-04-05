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

	"github.com/google/uuid"
	"swarm/internal/events"
	"swarm/internal/runtime/core/timeridentity"
	runtimedeadletters "swarm/internal/runtime/deadletters"
	runtimeengine "swarm/internal/runtime/engine"
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
	eventReceiptsCapability func(context.Context) (bool, error)

	testSubscribeHook   func()
	testEntityStateHook func(entityID, state string)
}

type PipelineCoordinatorOptions struct {
	ShardPlanner            any
	Module                  WorkflowModule
	InstanceActivator       FlowInstanceActivator
	InstanceDeactivator     FlowInstanceDeactivator
	TimerScheduler          *Scheduler
	TimerScheduleStore      SchedulePersistence
	EventReceiptsCapability func(context.Context) (bool, error)
}

func NewPipelineCoordinatorWithOptions(bus Bus, db *sql.DB, opts PipelineCoordinatorOptions) *PipelineCoordinator {
	if bus == nil {
		return nil
	}
	module := opts.Module
	if module == nil {
		panic("pipeline: workflow module is required")
	}
	return &PipelineCoordinator{
		bus:                     bus,
		db:                      db,
		module:                  module,
		workflowStore:           NewWorkflowInstanceStore(db),
		expressionEval:          newWorkflowExpressionEvaluator(),
		instanceActivator:       opts.InstanceActivator,
		instanceDeactivator:     opts.InstanceDeactivator,
		timerScheduler:          opts.TimerScheduler,
		timerScheduleStore:      opts.TimerScheduleStore,
		eventReceiptsCapability: opts.EventReceiptsCapability,
		entityLocks:             make(map[string]*sync.Mutex),
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
					EventID:   strings.TrimSpace(evt.ID),
					EventType: strings.TrimSpace(string(evt.Type)),
					EntityID:  workflowEventEntityID(evt),
					Error:     strings.TrimSpace(err.Error()),
				})
			}
		}
	}
}

func (pc *PipelineCoordinator) RunMaintenance(context.Context) {}

func (pc *PipelineCoordinator) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	if pc == nil {
		return true, nil, nil
	}
	eventType := strings.TrimSpace(string(evt.Type))
	if eventType == "" {
		return true, nil, nil
	}
	consume, handled := pc.interceptPolicy(eventType, evt)
	if !handled {
		return true, nil, nil
	}
	emitted := make([]events.Event, 0, 4)
	ictx := context.WithValue(ctx, pipelineEmitCollectorKey{}, &emitted)
	handled, err := pc.handleEventResult(ictx, evt)
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

func (pc *PipelineCoordinator) interceptPolicy(eventType string, evt events.Event) (consume bool, handled bool) {
	if strings.TrimSpace(eventType) == "" {
		return false, false
	}
	return pc.workflowNodeInterceptPolicy(eventType, evt)
}

func (pc *PipelineCoordinator) subscribe() <-chan events.Event {
	return pc.bus.Subscribe(runtimeWorkflowID, workflowSubscriptions(pc.WorkflowNodes())...)
}

func (pc *PipelineCoordinator) handleEvent(ctx context.Context, evt events.Event) bool {
	handled, _ := pc.handleEventResult(ctx, evt)
	return handled
}

func (pc *PipelineCoordinator) handleEventResult(ctx context.Context, evt events.Event) (bool, error) {
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
	trigger := strings.TrimSpace(string(evt.Type))
	if trigger == "" {
		return false, nil
	}
	handler, ok := source.NodeEventHandler(nodeID, trigger)
	if !ok && isAccumulationTimeoutEvent(events.EventType(trigger)) {
		bucket, bucketOK := timeridentity.ParseAccumulatorBucketRef(parsePayloadMap(evt.Payload))
		if bucketOK && strings.TrimSpace(bucket.NodeID) == nodeID {
			handler, ok = findAccumulationTimeoutHandlerForBucket(source, bucket)
		}
	}
	if !ok {
		return false, nil
	}
	ctx = withPipelineFlowScope(ctx, workflowNodeFlowID(source, nodeID))
	result, err := pc.executeNodeContractHandler(ctx, nodeID, handler, workflowTriggerContext{
		Event: evt,
		State: pc.currentWorkflowState(ctx, workflowEventEntityID(evt)),
	}, false)
	if err != nil {
		if errors.Is(err, runtimeengine.ErrChainDepthExceeded) {
			_ = runtimedeadletters.Insert(ctx, pc.db, runtimedeadletters.Record{
				OriginalEventID: strings.TrimSpace(evt.ID),
				FailureType:     "chain_depth_exceeded",
				ErrorMessage:    strings.TrimSpace(err.Error()),
				ChainDepth:      evt.ChainDepth,
				HandlerNode:     nodeID,
			})
			setPipelineReceiptOverride(ctx, "dead_letter", err.Error())
			return true, nil
		}
		pc.recordWorkflowHandlerFailure(ctx, evt, nodeID, err)
		return true, err
	}
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
	errText := strings.TrimSpace(err.Error())
	if errText == "" {
		errText = "unknown handler error"
	}
	setPipelineReceiptOverride(ctx, "dead_letter", errText)
	if pc.bus != nil {
		pc.bus.LogRuntime(ctx, RuntimeLogEntry{
			Level:     "error",
			Message:   "Workflow handler execution failed",
			Component: runtimeWorkflowID,
			Action:    "handler_error",
			EventID:   strings.TrimSpace(evt.ID),
			EventType: strings.TrimSpace(string(evt.Type)),
			EntityID:  workflowEventEntityID(evt),
			Error:     errText,
			Detail: map[string]any{
				"node_id": nodeID,
			},
		})
	}
	if pc.db != nil {
		_ = runtimedeadletters.Insert(ctx, pc.db, runtimedeadletters.Record{
			OriginalEventID: strings.TrimSpace(evt.ID),
			OriginalEvent:   strings.TrimSpace(string(evt.Type)),
			OriginalPayload: evt.Payload,
			EntityID:        workflowEventEntityID(evt),
			FlowInstance:    "runtime",
			FailureType:     "handler_error",
			ErrorMessage:    errText,
			RetryCount:      0,
			ChainDepth:      evt.ChainDepth,
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
		eventType := strings.TrimSpace(string(intercepted.Event.Type))
		errMsg := fmt.Sprintf("emit %s exceeded chain depth limit", eventType)
		rec := runtimedeadletters.Record{
			OriginalEventID: strings.TrimSpace(trigger.ID),
			OriginalEvent:   eventType,
			OriginalPayload: intercepted.Event.Payload,
			EntityID:        entityID,
			FlowInstance:    "runtime",
			FailureType:     "chain_depth_exceeded",
			ErrorMessage:    errMsg,
			ChainDepth:      intercepted.ChainDepth,
			HandlerNode:     firstNonEmptyString(nodeID+":"+eventType, nodeID),
		}
		recordDeadLetter := func() {
			if pc.db == nil {
				return
			}
			if err := runtimedeadletters.Insert(ctx, pc.db, rec); err != nil {
				pc.logRuntimeWarn(ctx, "workflow-runtime", "intercepted_emit_dead_letter_persist_failed", strings.TrimSpace(trigger.ID), strings.TrimSpace(string(trigger.Type)), runtimeWorkflowID, entityID, map[string]any{
					"intercepted_event_type": eventType,
					"handler_node":           nodeID,
				}, err)
			}
		}
		if !queuePipelinePostCommitAction(ctx, recordDeadLetter) {
			recordDeadLetter()
		}
		deadLetterPayload := map[string]any{
			"original_event":   eventType,
			"original_payload": json.RawMessage(intercepted.Event.Payload),
			"entity_id":        entityID,
			"flow_instance":    "runtime",
			"failure_type":     "chain_depth_exceeded",
			"error_message":    errMsg,
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
			*collector = append(*collector, (events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("platform.dead_letter"),
				SourceAgent: sourceAgent,
				Payload:     mustJSON(deadLetterPayload),
				CreatedAt:   time.Now().UTC(),
			}).WithEntityID(entityID))
			continue
		}
		publishDeadLetter := func() {
			if err := pc.publish(ctx, "platform.dead_letter", entityID, deadLetterPayload); err != nil {
				pc.logRuntimeWarn(ctx, "workflow-runtime", "intercepted_emit_dead_letter_publish_failed", strings.TrimSpace(trigger.ID), strings.TrimSpace(string(trigger.Type)), runtimeWorkflowID, entityID, map[string]any{
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
		errText := ""
		if err != nil {
			errText = strings.TrimSpace(err.Error())
		}
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
			Error:     errText,
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
	emitted := (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(strings.TrimSpace(eventType)),
		SourceAgent: sourceAgent,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(entityID)
	if flowInstance := strings.Trim(strings.TrimSpace(asString(payload["flow_instance"])), "/"); flowInstance != "" {
		emitted = emitted.WithFlowInstance(flowInstance)
	}
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
	emitted := (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(strings.TrimSpace(eventType)),
		SourceAgent: sourceAgent,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(entityID)
	if flowInstance := strings.Trim(strings.TrimSpace(asString(payload["flow_instance"])), "/"); flowInstance != "" {
		emitted = emitted.WithFlowInstance(flowInstance)
	}
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
