package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	"empireai/internal/runtime/semanticview"
)

const (
	maxRevisionCycles  = 3
	maxInnerRevisions  = 5
	packagingTimeout   = 30 * time.Minute
	scanTimeout        = 90 * time.Minute
	scoringTimeout     = 60 * time.Minute
	maxEntityNameLen   = 96
	maxEntitySlugLen   = 72

	// Narrative fields are only last-resort naming fallbacks and should stay concise.
	maxNarrativeFallbackNameLen   = 72
	maxNarrativeFallbackNameWords = 8

	localServicesScannerExpected = 5
	corpusBatchSize              = 25
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
		for _, node := range pc.WorkflowNodes() {
			if _, ok := node.Policies[eventType]; ok {
				if nodeID := strings.TrimSpace(node.ID); nodeID != "" {
					return nodeID
				}
			}
		}
	}
	return runtimeWorkflowID
}

// PipelineCoordinator handles deterministic workflow state-machine work
// (scan assignment/aggregation and validation gate orchestration) so agents
// can focus on judgment tasks.
type FactoryPipelineCoordinator struct {
	bus Bus
	db  *sql.DB

	mu sync.Mutex

	entityLockMu sync.Mutex
	entityLocks  map[string]*sync.Mutex

	scanCoordinator *ScanCoordinator
	scoringState    *ScoringState
	validationGate  *ValidationGate
	module          WorkflowModule
	discoveryPolicy DiscoveryPolicy
	scoringPolicy   ScoringPolicy
	payloads        PayloadFactory
	processed       map[string]struct{}
	workflowStore   *WorkflowInstanceStore
	stateLoaded     bool

	statePersistenceChecked bool
	statePersistenceEnabled bool

	shardPlanner       *ShardPlanner
	payloadFactory     *PipelinePayloadFactory
	expressionEval     *workflowExpressionEvaluator
	instanceActivator  FlowInstanceActivator
	timerScheduler     *Scheduler
	timerScheduleStore SchedulePersistence
	shardsTableChecked bool
	shardsTableEnabled bool

	scoringDigestBufferChecked bool
	scoringDigestBufferEnabled bool
	lastScoringDigestReadAt    time.Time

	testSubscribeHook  func()
	testEntityStateHook func(entityID, state string)
}

type FactoryPipelineCoordinatorOptions struct {
	ShardPlanner      *ShardPlanner
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

func NewFactoryPipelineCoordinatorWithOptions(bus Bus, db *sql.DB, opts FactoryPipelineCoordinatorOptions) *FactoryPipelineCoordinator {
	if bus == nil {
		return nil
	}
	module := opts.Module
	if module == nil {
		module = defaultWorkflowModule()
	}
	discoveryPolicy := workflowModuleDiscoveryPolicy(module)
	scoringPolicy := workflowModuleScoringPolicy(module)
	payloads := workflowModulePayloadFactory(module)
	pc := &FactoryPipelineCoordinator{
		bus:               bus,
		db:                db,
		scanCoordinator:   NewScanCoordinator(),
		scoringState:      NewScoringState(),
		validationGate:    NewValidationGate(),
		module:            module,
		discoveryPolicy:   discoveryPolicy,
		scoringPolicy:     scoringPolicy,
		payloads:          payloads,
		processed:         make(map[string]struct{}),
		entityLocks:       make(map[string]*sync.Mutex),
		shardPlanner:      opts.ShardPlanner,
		expressionEval:    newWorkflowExpressionEvaluator(),
		instanceActivator: opts.InstanceActivator,
	}
	pc.workflowStore = NewWorkflowInstanceStore(db)
	pc.scanCoordinator.runtime = pc
	pc.scanCoordinator.discovery = discoveryPolicy
	pc.scoringState.runtime = pc
	pc.scoringState.scoring = scoringPolicy
	pc.payloadFactory = NewPipelinePayloadFactory(payloads, scoringPolicy, pc)
	pc.scanCoordinator.payloadFactory = pc.payloadFactory
	pc.scoringState.payloadFactory = pc.payloadFactory
	pc.validationGate.payloadFactory = pc.payloadFactory
	pc.scanCoordinator.mu = &pc.mu
	pc.scoringState.mu = &pc.mu
	pc.validationGate.mu = &pc.mu
	if db != nil {
		ctx := context.Background()
		pc.statePersistenceEnabled = detectStatePersistence(ctx, db)
		pc.statePersistenceChecked = true
		pc.shardsTableEnabled = detectShardsTable(ctx, db)
		pc.shardsTableChecked = true
		pc.scoringDigestBufferEnabled = detectScoringDigestBuffer(ctx, db)
		pc.scoringDigestBufferChecked = true
	}
	return pc
}

func NewPipelineCoordinatorWithOptions(bus Bus, db *sql.DB, opts PipelineCoordinatorOptions) *PipelineCoordinator {
	return NewFactoryPipelineCoordinatorWithOptions(bus, db, opts)
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

type scoringAccumulator struct {
	EntityID         string
	EntityName       string
	Geography        string
	GeographicScope  string
	Mode             string
	Rubric           string
	DiscoveryContext map[string]any
	Expected         []string
	Received         map[string]scoreDimensionResult
	Contested        map[string]contestedDimension
	RequestedAt      time.Time
	LastUpdatedAt    time.Time
	ContestNotified  map[string]bool
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

type validationPipelineState struct {
	EntityID string
	Status     string

	G1Research bool
	G2Spec     bool
	G3CTO      bool
	G4Brand    bool

	ResearchPayload json.RawMessage
	SpecPayload     json.RawMessage
	CTOPayload      json.RawMessage
	BrandPayload    json.RawMessage
	ScoringPayload  json.RawMessage

	RevisionCount      int
	InnerRevisionCount int
	SpecVersion        int

	PackagingRequested   bool
	PackagingRequestedAt *time.Time
	PackagingRetries     int
}

func (st *validationPipelineState) gateContext() map[string]any {
	if st == nil {
		return nil
	}
	return map[string]any{
		"g1_research": st.G1Research,
		"g2_spec":     st.G2Spec,
		"g3_cto":      st.G3CTO,
		"g4_brand":    st.G4Brand,
	}
}

func NewFactoryPipelineCoordinator(bus Bus, db *sql.DB) *FactoryPipelineCoordinator {
	return NewFactoryPipelineCoordinatorWithOptions(bus, db, FactoryPipelineCoordinatorOptions{Module: defaultWorkflowModule()})
}

func NewPipelineCoordinator(bus Bus, db *sql.DB) *PipelineCoordinator {
	return NewFactoryPipelineCoordinator(bus, db)
}

func (pc *FactoryPipelineCoordinator) SetShardPlanner(planner *ShardPlanner) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.shardPlanner = planner
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
				pc.resetInMemoryState()
				ch = pc.subscribe()
				pc.notifyTestSubscribed()
				continue
			}
			pc.handleEvent(ctx, evt)
		}
	}
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

func (pc *FactoryPipelineCoordinator) RunMaintenance(ctx context.Context) {
	if pc == nil {
		return
	}
	pc.ensureStateLoaded(ctx)
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pc.checkScanTimeouts(ctx, time.Now().UTC())
			pc.checkScoringTimeouts(ctx, time.Now().UTC())
			pc.persistRuntimeState(ctx)
		}
	}
}

// Intercept executes deterministic pipeline transitions in the EventBus publish path.
// Returns passthrough=false when the event should be consumed.
func (pc *FactoryPipelineCoordinator) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	if pc == nil {
		return true, nil, nil
	}
	pc.ensureStateLoaded(ctx)
	defer pc.persistRuntimeState(ctx)
	startedAt := time.Now()
	eventType := strings.TrimSpace(string(evt.Type))
	if eventType == "" {
		return true, nil, nil
	}
	payload := parsePayloadMap(evt.Payload)
	before := pc.transitionStateSnapshot(eventType, evt, payload)

	record := func(action, dropReason string, emitted []events.Event, execErr error) {
		pc.recordTransition(ctx, startedAt, eventType, evt, payload, before, action, dropReason, emitted, execErr)
	}

	if dropReason := pc.interceptStateDropReason(eventType, evt); dropReason != "" {
		record("dropped", dropReason, nil, nil)
		if consume, handled := pc.interceptPolicy(eventType, evt); handled && consume {
			return false, nil, nil
		}
		return true, nil, nil
	}

	consume, handled := pc.interceptPolicy(eventType, evt)
	if !handled {
		if dropReason := pc.interceptDropReason(eventType, evt); dropReason != "" {
			record("dropped", dropReason, nil, nil)
		}
		return true, nil, nil
	}

	emitted := make([]events.Event, 0, 4)
	ictx := context.WithValue(ctx, pipelineEmitCollectorKey{}, &emitted)
	pc.handleEvent(ictx, evt)

	// Opportunistic timer checks while events are flowing.
	pc.checkScanTimeouts(ictx, time.Now().UTC())
	pc.checkScoringTimeouts(ictx, time.Now().UTC())

	if consume {
		record("consumed", "", emitted, nil)
	} else {
		record("processed", "", emitted, nil)
	}
	return !consume, emitted, nil
}

func (pc *FactoryPipelineCoordinator) interceptDropReason(eventType string, evt events.Event) string {
	switch eventType {
	case "spec.validation_passed", "spec.validation_failed", "vertical.ready_for_review", "spec.revision_needed":
		if workflowEventEntityID(evt) == "" {
			return "missing entity_id"
		}
	}
	return ""
}

func (pc *FactoryPipelineCoordinator) interceptStateDropReason(eventType string, evt events.Event) string {
	verticalID := workflowEventEntityID(evt)
	if verticalID == "" {
		return ""
	}

	st := pc.validationStateSnapshot(verticalID)

	switch eventType {
	case "vertical.shortlisted":
		if st != nil {
			return "pipeline already exists"
		}
		return ""
	}

	if st == nil {
		return ""
	}
	status := strings.TrimSpace(st.Status)
	if status == "" || status == "active" {
		return ""
	}

	switch eventType {
	case "research.completed",
		"spec.approved",
		"cto.spec_approved",
		"brand.candidates_ready",
		"spec.validation_passed",
		"spec.validation_failed",
		"cto.spec_revision_needed",
		"spec.revision_requested",
		"spec.revision_needed",
		"brand.revision_needed":
		return "status=" + status + ", expected=active"
	default:
		return ""
	}
}

func (pc *FactoryPipelineCoordinator) interceptPolicy(eventType string, evt events.Event) (consume bool, handled bool) {
	if strings.TrimSpace(eventType) == "" {
		return false, false
	}
	if consume, handled := pc.workflowNodeInterceptPolicy(eventType, evt); handled {
		return consume, true
	}
	return false, false
}

func (pc *FactoryPipelineCoordinator) subscribe() <-chan events.Event {
	return pc.bus.Subscribe(runtimeWorkflowID, workflowSubscriptions(pc.WorkflowNodes())...)
}

func (pc *FactoryPipelineCoordinator) handleEvent(ctx context.Context, evt events.Event) {
	if strings.TrimSpace(evt.ID) == "" {
		return
	}
	if !pc.markEventProcessed(ctx, evt.ID) {
		return
	}

	switch string(evt.Type) {
	case "runtime.reset":
		_ = pc.dispatchWorkflowNodeEvent(ctx, evt)
		return
	default:
		_ = pc.dispatchWorkflowNodeEvent(ctx, evt)
	}
}

func (pc *FactoryPipelineCoordinator) resetInMemoryState() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.scanCoordinator.scans = make(map[string]*scanAccumulator)
	pc.scoringState.accumulators = make(map[string]*scoringAccumulator)
	pc.scanCoordinator.pendingDedup = make(map[string]pendingCandidate)
	pc.validationGate.states = make(map[string]*validationPipelineState)
	pc.processed = make(map[string]struct{})
	pc.stateLoaded = true
}
