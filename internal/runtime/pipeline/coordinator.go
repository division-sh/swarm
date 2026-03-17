package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	runtimedeadletters "empireai/internal/runtime/deadletters"
	runtimeengine "empireai/internal/runtime/engine"
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

type FactoryPipelineCoordinator struct {
	bus Bus
	db  *sql.DB

	mu sync.Mutex

	entityLockMu sync.Mutex
	entityLocks  map[string]*sync.Mutex

	module              WorkflowModule
	workflowStore       *WorkflowInstanceStore
	expressionEval      *workflowExpressionEvaluator
	instanceActivator   FlowInstanceActivator
	instanceDeactivator FlowInstanceDeactivator
	timerScheduler      *Scheduler
	timerScheduleStore  SchedulePersistence

	testSubscribeHook   func()
	testEntityStateHook func(entityID, state string)
}

type FactoryPipelineCoordinatorOptions struct {
	ShardPlanner        any
	Module              WorkflowModule
	InstanceActivator   FlowInstanceActivator
	InstanceDeactivator FlowInstanceDeactivator
}

type PipelineCoordinator = FactoryPipelineCoordinator
type PipelineCoordinatorOptions = FactoryPipelineCoordinatorOptions

func NewFactoryPipelineCoordinatorWithOptions(bus Bus, db *sql.DB, opts FactoryPipelineCoordinatorOptions) *FactoryPipelineCoordinator {
	if bus == nil {
		return nil
	}
	module := opts.Module
	if module == nil {
		module = defaultWorkflowModule()
	}
	return &FactoryPipelineCoordinator{
		bus:                 bus,
		db:                  db,
		module:              module,
		workflowStore:       NewWorkflowInstanceStore(db),
		expressionEval:      newWorkflowExpressionEvaluator(),
		instanceActivator:   opts.InstanceActivator,
		instanceDeactivator: opts.InstanceDeactivator,
		entityLocks:         make(map[string]*sync.Mutex),
	}
}

func NewPipelineCoordinatorWithOptions(bus Bus, db *sql.DB, opts PipelineCoordinatorOptions) *PipelineCoordinator {
	return NewFactoryPipelineCoordinatorWithOptions(bus, db, opts)
}

func NewFactoryPipelineCoordinator(bus Bus, db *sql.DB) *FactoryPipelineCoordinator {
	return NewFactoryPipelineCoordinatorWithOptions(bus, db, FactoryPipelineCoordinatorOptions{Module: defaultWorkflowModule()})
}

func NewPipelineCoordinator(bus Bus, db *sql.DB) *PipelineCoordinator {
	return NewFactoryPipelineCoordinator(bus, db)
}

func (pc *FactoryPipelineCoordinator) SetTestSubscribeHook(fn func()) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.testSubscribeHook = fn
	pc.mu.Unlock()
}

func (pc *FactoryPipelineCoordinator) SetTestEntityStateHook(fn func(entityID, state string)) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.testEntityStateHook = fn
	pc.mu.Unlock()
}

func (pc *FactoryPipelineCoordinator) SetInstanceActivator(activator FlowInstanceActivator) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.instanceActivator = activator
	pc.mu.Unlock()
}

func (pc *FactoryPipelineCoordinator) SetInstanceDeactivator(deactivator FlowInstanceDeactivator) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.instanceDeactivator = deactivator
	pc.mu.Unlock()
}

func (pc *FactoryPipelineCoordinator) SetTimerScheduling(scheduler *Scheduler, store SchedulePersistence) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.timerScheduler = scheduler
	pc.timerScheduleStore = store
	pc.mu.Unlock()
}

func (pc *FactoryPipelineCoordinator) Run(ctx context.Context) {
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
			pc.handleEvent(ctx, evt)
		}
	}
}

func (pc *FactoryPipelineCoordinator) RunMaintenance(context.Context) {}

func (pc *FactoryPipelineCoordinator) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
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
	if !pc.handleEvent(ictx, evt) {
		return true, nil, nil
	}
	return !consume, emitted, nil
}

func (pc *FactoryPipelineCoordinator) interceptPolicy(eventType string, evt events.Event) (consume bool, handled bool) {
	if strings.TrimSpace(eventType) == "" {
		return false, false
	}
	return pc.workflowNodeInterceptPolicy(eventType, evt)
}

func (pc *FactoryPipelineCoordinator) subscribe() <-chan events.Event {
	return pc.bus.Subscribe(runtimeWorkflowID, workflowSubscriptions(pc.WorkflowNodes())...)
}

func (pc *FactoryPipelineCoordinator) handleEvent(ctx context.Context, evt events.Event) bool {
	return pc.dispatchWorkflowNodeEvent(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) executeNodeHandlerPlan(ctx context.Context, nodeID string, evt events.Event) bool {
	if pc == nil {
		return false
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return false
	}
	source := pc.SemanticSource()
	if source == nil {
		return false
	}
	trigger := strings.TrimSpace(string(evt.Type))
	if trigger == "" {
		return false
	}
	handler, ok := source.NodeEventHandler(nodeID, trigger)
	if !ok && isAccumulationTimeoutEvent(events.EventType(trigger)) {
		handler, ok = findAccumulationTimeoutHandlerForNode(source, nodeID, trigger)
	}
	if !ok {
		return false
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
			return true
		}
		return false
	}
	if result.Handled {
		pc.reconcileWorkflowEventTimers(ctx, workflowEventEntityID(evt), trigger)
	}
	return result.Handled
}

func (pc *FactoryPipelineCoordinator) notifyTestSubscribed() {
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

func (pc *FactoryPipelineCoordinator) notifyTestEntityStateUpdated(entityID, state string) {
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

func (pc *FactoryPipelineCoordinator) publish(ctx context.Context, eventType, entityID string, payload map[string]any) {
	if pc == nil {
		return
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
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		*collector = append(*collector, emitted)
		return
	}
	if pc.bus != nil {
		_ = pc.bus.Publish(ctx, emitted)
	}
}

func (pc *FactoryPipelineCoordinator) publishDirect(ctx context.Context, eventType, entityID string, payload map[string]any, recipients []string) {
	if pc == nil {
		return
	}
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		pc.publish(ctx, eventType, entityID, payload)
		return
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
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		*collector = append(*collector, emitted)
		return
	}
	if pc.bus != nil {
		_ = pc.bus.PublishDirect(ctx, emitted, recipients)
	}
}
