package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/core/paths"
	"empireai/internal/runtime/semanticview"
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

type workflowTransitionOutcome struct {
	Transition       WorkflowTransition
	PreviousState    WorkflowState
	CurrentState     WorkflowState
	GuardsEvaluated  []string
	ActionsExecuted  []string
	TriggerEventID   string
	TriggerEventType string
}

type WorkflowHookContext struct {
	Event    events.Event
	EntityID string
	Payload  map[string]any
	State    WorkflowState
}

type workflowTriggerContext struct {
	Event events.Event
	State WorkflowState
}

type handlerExecutionPlan struct {
	NodeID           string
	EventType        string
	Guard            string
	GuardSpec        *runtimecontracts.GuardSpec
	Action           string
	Template         string
	InstanceIDFrom   string
	InstanceIDPath   paths.Path
	ConfigFrom       *runtimecontracts.ConfigFromSpec
	CompletionRule   string
	Accumulate       *runtimecontracts.AccumulateSpec
	Compute          *runtimecontracts.ComputeSpec
	FanOut           *runtimecontracts.FanOutSpec
	AdvancesTo       string
	SetsGate         string
	ClearGates       bool
	DataAccumulation runtimecontracts.WorkflowDataAccumulation
	PayloadTransform *runtimecontracts.PayloadTransformSpec
	Emits            string
	EmitEvents       []string
	Rules            []runtimecontracts.HandlerRuleEntry
	OnComplete       []runtimecontracts.HandlerRuleEntry
	ExecutionOrder   []string
}

func workflowEventEntityID(evt events.Event) string {
	return workflowEventEntityIDWithPayload(evt, parsePayloadMap(evt.Payload))
}

func workflowEventEntityIDWithPayload(evt events.Event, payload map[string]any) string {
	return strings.TrimSpace(firstNonEmptyString(
		asString(payload["entity_id"]),
		asString(payload["vertical_id"]),
		evt.EntityID(),
	))
}

func workflowHookContextFromTrigger(triggerCtx workflowTriggerContext) WorkflowHookContext {
	return WorkflowHookContext{
		Event:    triggerCtx.Event,
		EntityID: workflowEventEntityID(triggerCtx.Event),
		Payload:  parsePayloadMap(triggerCtx.Event.Payload),
		State:    triggerCtx.State,
	}
}

func handlerGuardID(spec *runtimecontracts.GuardSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.ID)
}

func handlerExecutionPlanFromNodeHandler(nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) handlerExecutionPlan {
	plan := handlerExecutionPlan{
		NodeID:           strings.TrimSpace(nodeID),
		EventType:        strings.TrimSpace(eventType),
		Guard:            handlerGuardID(handler.Guard),
		GuardSpec:        handler.Guard,
		Action:           strings.TrimSpace(handler.Action.ID),
		Template:         strings.TrimSpace(handler.Action.Template),
		InstanceIDFrom:   strings.TrimSpace(handler.Action.InstanceIDFrom),
		InstanceIDPath:   handler.Action.InstanceIDPath,
		ConfigFrom:       handler.Action.ConfigFrom,
		CompletionRule:   strings.TrimSpace(handler.CompletionRule),
		Accumulate:       handler.Accumulate,
		Compute:          handler.Compute,
		FanOut:           handler.FanOut,
		AdvancesTo:       strings.TrimSpace(handler.AdvancesTo),
		SetsGate:         gateSpecString(handler.SetsGate),
		ClearGates:       len(handler.ClearGates) > 0,
		DataAccumulation: handler.DataAccumulation,
		PayloadTransform: handler.PayloadTransform,
		Emits:            strings.TrimSpace(handler.Emits.First()),
		EmitEvents:       handler.Emits.Values(),
		Rules:            append([]runtimecontracts.HandlerRuleEntry(nil), handler.Rules...),
		OnComplete:       append([]runtimecontracts.HandlerRuleEntry(nil), handler.OnComplete...),
	}
	plan.ExecutionOrder = handlerExecutionOrderForPlan(plan)
	return plan
}

func handlerExecutionOrderForPlan(plan handlerExecutionPlan) []string {
	steps := make([]string, 0, 12)
	if plan.ClearGates {
		steps = append(steps, "clear_gates")
	}
	if strings.TrimSpace(plan.Guard) != "" || plan.GuardSpec != nil {
		steps = append(steps, "guard")
	}
	if plan.Accumulate != nil {
		steps = append(steps, "accumulate")
	}
	if plan.Compute != nil {
		steps = append(steps, "compute")
	}
	if plan.FanOut != nil {
		steps = append(steps, "fan_out")
	}
	if len(plan.OnComplete) > 0 {
		steps = append(steps, "on_complete")
	}
	if len(plan.Rules) > 0 {
		steps = append(steps, "rules")
	}
	if plan.AdvancesTo != "" {
		steps = append(steps, "advances_to")
	}
	if plan.SetsGate != "" {
		steps = append(steps, "sets_gate")
	}
	if plan.DataAccumulation.HasWrites() || strings.TrimSpace(plan.DataAccumulation.SourceEvent) != "" {
		steps = append(steps, "data_accumulation")
	}
	if plan.PayloadTransform != nil {
		steps = append(steps, "payload_transform")
	}
	if len(plan.EmitEvents) > 0 || plan.Emits != "" {
		steps = append(steps, "emits")
	}
	if plan.Action != "" {
		steps = append(steps, "action")
	}
	return steps
}

type entityCandidate struct {
	ID        string
	Name      string
	Geography string
}

type pendingCandidate struct {
	DedupEventID string
	ExistingID   string
	ScanID       string
	CampaignID   string
	Mode         string
	Geography    string
	Name         string
	Signal       float64
	Payload      map[string]any
}

type scanAccumulator struct {
	ScanID      string
	CampaignID  string
	Mode        string
	Geography   string
	Expected    int
	CompletedBy map[string]struct{}
	ReportData  []map[string]any
	Reports     int
	Discovered  int
	Skipped     int
	CreatedAt   time.Time
}

type validationPipelineState struct {
	EntityID           string
	Status             string
	G1Research         bool
	G2Spec             bool
	G3CTO              bool
	G4Brand            bool
	RevisionCount      int
	InnerRevisionCount int
	SpecVersion        int
}

func (s *validationPipelineState) gateContext() map[string]bool {
	if s == nil {
		return map[string]bool{}
	}
	return map[string]bool{
		"g1_research": s.G1Research,
		"g2_spec":     s.G2Spec,
		"g3_cto":      s.G3CTO,
		"g4_brand":    s.G4Brand,
	}
}

type ScanCoordinator struct {
	runtime      scanWorkflowRuntime
	pendingDedup map[string]pendingCandidate
}

func NewScanCoordinator() *ScanCoordinator {
	return &ScanCoordinator{pendingDedup: make(map[string]pendingCandidate)}
}

func (*ScanCoordinator) handlePortfolioDigestTimer(context.Context, events.Event) {}
func (*ScanCoordinator) handleScanRequested(context.Context, events.Event)         {}
func (*ScanCoordinator) handleDiscoveryReport(context.Context, events.Event)       {}
func (*ScanCoordinator) handleScanCompletion(context.Context, events.Event)        {}
func (*ScanCoordinator) handleDedupResolved(context.Context, events.Event)         {}
func (*ScanCoordinator) handleSynthesisResolved(context.Context, events.Event)     {}
func (*ScanCoordinator) handleScanTimeout(context.Context, events.Event)           {}
func (*ScanCoordinator) handleCampaignDeadline(context.Context, events.Event)      {}
func (*ScanCoordinator) checkTimeouts(context.Context, time.Time)                  {}

type ScoringState struct{}

func NewScoringState() *ScoringState { return &ScoringState{} }

type ValidationGate struct {
	states map[string]*validationPipelineState
}

func NewValidationGate() *ValidationGate {
	return &ValidationGate{states: make(map[string]*validationPipelineState)}
}

type FactoryPipelineCoordinator struct {
	bus Bus
	db  *sql.DB

	mu sync.Mutex

	entityLockMu sync.Mutex
	entityLocks  map[string]*sync.Mutex

	scanCoordinator *ScanCoordinator
	scoringState    *ScoringState
	validationGate  *ValidationGate
	module            WorkflowModule
	workflowStore     *WorkflowInstanceStore
	expressionEval    *workflowExpressionEvaluator
	instanceActivator FlowInstanceActivator
	timerScheduler    *Scheduler
	timerScheduleStore SchedulePersistence

	testSubscribeHook   func()
	testEntityStateHook func(entityID, state string)
}

type FactoryPipelineCoordinatorOptions struct {
	ShardPlanner      any
	Module            WorkflowModule
	InstanceActivator FlowInstanceActivator
}

type PipelineCoordinator = FactoryPipelineCoordinator
type PipelineCoordinatorOptions = FactoryPipelineCoordinatorOptions

type FlowInstanceActivationRequest struct {
	ContractBundle semanticview.Source
	TemplateID     string
	InstanceID     string
	EntityID       string
	FlowPath       string
	InitialState   string
	Config         map[string]any
	TriggerEvent   events.Event
}

type FlowInstanceActivator func(context.Context, FlowInstanceActivationRequest) error

func (pc *FactoryPipelineCoordinator) runtimeHandlerID(eventType string) string {
	eventType = strings.TrimSpace(eventType)
	if eventType != "" {
		if source := pc.SemanticSource(); source != nil {
			if entry, ok := source.EventEntry(eventType); ok {
				if owner := strings.TrimSpace(entry.OwningNode); owner != "" {
					return owner
				}
			}
		}
	}
	return runtimeWorkflowID
}

func NewFactoryPipelineCoordinatorWithOptions(bus Bus, db *sql.DB, opts FactoryPipelineCoordinatorOptions) *FactoryPipelineCoordinator {
	if bus == nil {
		return nil
	}
	module := opts.Module
	if module == nil {
		module = defaultWorkflowModule()
	}
	pc := &FactoryPipelineCoordinator{
		bus:               bus,
		db:                db,
		scanCoordinator:   NewScanCoordinator(),
		scoringState:      NewScoringState(),
		validationGate:    NewValidationGate(),
		module:            module,
		workflowStore:     NewWorkflowInstanceStore(db),
		expressionEval:    newWorkflowExpressionEvaluator(),
		instanceActivator: opts.InstanceActivator,
		entityLocks:       make(map[string]*sync.Mutex),
	}
	if pc.scanCoordinator != nil {
		pc.scanCoordinator.runtime = pc
	}
	return pc
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

func (pc *FactoryPipelineCoordinator) SetTestVerticalStageHook(fn func(verticalID, stage string)) {
	pc.SetTestEntityStateHook(fn)
}

func (pc *FactoryPipelineCoordinator) SetInstanceActivator(activator FlowInstanceActivator) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.instanceActivator = activator
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

func (pc *FactoryPipelineCoordinator) SetShardPlanner(any) {}

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
	result, err := pc.executeAuthoritativeNodeHandler(ctx, evt, workflowTriggerContext{
		Event: evt,
		State: pc.currentWorkflowState(ctx, workflowEventEntityID(evt)),
	})
	if err != nil {
		return false
	}
	return result.Handled && strings.TrimSpace(result.Plan.NodeID) == strings.TrimSpace(nodeID)
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

func (pc *FactoryPipelineCoordinator) currentWorkflowState(ctx context.Context, entityID string) WorkflowState {
	entityID = strings.TrimSpace(entityID)
	state := WorkflowState{
		EntityID: entityID,
		Stage:    NormalizeWorkflowStateID(""),
		Metadata: map[string]any{},
	}
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || entityID == "" {
		return state
	}
	instance, ok, err := pc.workflowStore.Load(ctx, entityID)
	if err != nil || !ok {
		return state
	}
	state.Stage = NormalizeWorkflowStateID(instance.CurrentState)
	state.Metadata = cloneStringAnyMap(instance.Metadata)
	return state
}

func (pc *FactoryPipelineCoordinator) updateVerticalStage(ctx context.Context, entityID, stage, sourceEvent string) {
	if pc == nil {
		return
	}
	entityID = strings.TrimSpace(entityID)
	stage = strings.TrimSpace(string(NormalizeWorkflowStateID(stage)))
	if entityID == "" || stage == "" {
		return
	}
	current := pc.currentWorkflowState(ctx, entityID)
	currentStage := strings.TrimSpace(string(current.Stage))
	current.Stage = NormalizeWorkflowStateID(stage)
	current.EntityID = entityID
	pc.persistWorkflowStageProjection(ctx, entityID, currentStage, stage, strings.TrimSpace(sourceEvent), current)
	pc.notifyTestEntityStateUpdated(entityID, stage)
}

func (pc *FactoryPipelineCoordinator) applyWorkflowGateMutation(ctx context.Context, entityID, _sourceEvent, setGate string, clear bool) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	entityID = strings.TrimSpace(entityID)
	setGate = strings.TrimSpace(setGate)
	if entityID == "" {
		return
	}
	_ = pc.workflowStore.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
		metadata := cloneStringAnyMap(instance.Metadata)
		gates := payloadMap(metadata["gates"])
		if clear {
			for key := range gates {
				delete(gates, key)
			}
		}
		if setGate != "" {
			gates[setGate] = true
		}
		if len(gates) == 0 {
			delete(metadata, "gates")
		} else {
			metadata["gates"] = gates
		}
		instance.Metadata = metadata
	})
}

func (pc *FactoryPipelineCoordinator) recordWorkflowEvidence(ctx context.Context, entityID string, nodeID string, payload map[string]any) bool {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return false
	}
	entityID = strings.TrimSpace(entityID)
	nodeID = strings.TrimSpace(nodeID)
	if entityID == "" || nodeID == "" {
		return false
	}
	_ = pc.workflowStore.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
		bucket := workflowMutableStateBucket(instance, "evidence")
		bucket[nodeID] = cloneMap(payload)
		workflowSetStateBucket(instance, "evidence", bucket)
	})
	return true
}

func (pc *FactoryPipelineCoordinator) createFlowInstance(ctx context.Context, triggerCtx workflowTriggerContext, plan handlerExecutionPlan) bool {
	if pc == nil || pc.instanceActivator == nil {
		return false
	}
	templateID := strings.TrimSpace(plan.Template)
	if templateID == "" {
		return false
	}
	entityID := workflowEventEntityID(triggerCtx.Event)
	instanceID := strings.TrimSpace(firstNonEmptyString(
		asString(parsePayloadMap(triggerCtx.Event.Payload)["instance_id"]),
		plan.InstanceIDFrom,
	))
	if instanceID == "" {
		instanceID = uuid.NewString()
	}
	req := FlowInstanceActivationRequest{
		ContractBundle: pc.SemanticSource(),
		TemplateID:     templateID,
		InstanceID:     instanceID,
		EntityID:       entityID,
		InitialState:   strings.TrimSpace(plan.AdvancesTo),
		Config:         map[string]any{},
		TriggerEvent:   triggerCtx.Event,
	}
	if plan.ConfigFrom != nil {
		req.Config = cloneMap(parsePayloadMap(triggerCtx.Event.Payload))
	}
	return pc.instanceActivator(ctx, req) == nil
}

func (pc *FactoryPipelineCoordinator) handlerEmitPayload(_ context.Context, triggerCtx workflowTriggerContext, eventType string) map[string]any {
	payload := parsePayloadMap(triggerCtx.Event.Payload)
	out := cloneMap(payload)
	entityID := workflowEventEntityIDWithPayload(triggerCtx.Event, payload)
	if entityID != "" {
		out["entity_id"] = entityID
	}
	if strings.TrimSpace(eventType) != "" {
		out["trigger_event_type"] = strings.TrimSpace(string(triggerCtx.Event.Type))
	}
	if state := strings.TrimSpace(string(triggerCtx.State.Stage)); state != "" {
		out["current_state"] = state
	}
	return out
}

func (pc *FactoryPipelineCoordinator) lockWorkflowEntity(entityID string) func() {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return func() {}
	}
	pc.entityLockMu.Lock()
	lock, ok := pc.entityLocks[entityID]
	if !ok {
		lock = &sync.Mutex{}
		pc.entityLocks[entityID] = lock
	}
	pc.entityLockMu.Unlock()
	lock.Lock()
	return lock.Unlock
}

func (pc *FactoryPipelineCoordinator) validationStateSnapshot(entityID string) *validationPipelineState {
	if pc == nil || pc.validationGate == nil {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return nil
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	st := pc.validationGate.states[entityID]
	if st == nil {
		return nil
	}
	copied := *st
	return &copied
}

func (pc *FactoryPipelineCoordinator) mutateValidationState(_ context.Context, entityID string, mutate func(*validationPipelineState)) *validationPipelineState {
	if pc == nil || pc.validationGate == nil || mutate == nil {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return nil
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	st := pc.validationGate.states[entityID]
	if st == nil {
		st = &validationPipelineState{EntityID: entityID, Status: "active"}
		pc.validationGate.states[entityID] = st
	}
	mutate(st)
	copied := *st
	return &copied
}

func (pc *FactoryPipelineCoordinator) handlePortfolioDigestTimer(context.Context, events.Event) {}

func (pc *FactoryPipelineCoordinator) planAndPersistShards(context.Context, events.Event, string, string, map[string]any) int {
	return 0
}

func (pc *FactoryPipelineCoordinator) logPrefilterSkip(context.Context, events.Event, string, string, string, string, map[string]any, float64, float64) {
}

func (pc *FactoryPipelineCoordinator) markShardCompletedByAgent(context.Context, string) string {
	return ""
}

func (pc *FactoryPipelineCoordinator) shardTerminalProgress(context.Context, string) (int, int, int, bool) {
	return 0, 0, 0, false
}

func (pc *FactoryPipelineCoordinator) loadEntitiesByGeography(context.Context, string) ([]entityCandidate, error) {
	return nil, nil
}

func (pc *FactoryPipelineCoordinator) ensureEntityDiscovered(context.Context, string, string, string, map[string]any) (string, error) {
	return "", nil
}

func (pc *FactoryPipelineCoordinator) loadWorkflowScanProjection(context.Context, string) (*scanAccumulator, map[string]pendingCandidate, bool) {
	return nil, nil, false
}

func (pc *FactoryPipelineCoordinator) applyMarginalKillTimer(context.Context, events.Event) {}
