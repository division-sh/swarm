package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
)

const (
	maxRevisionCycles  = 3
	maxInnerRevisions  = 5
	packagingTimeout   = 30 * time.Minute
	scanTimeout        = 90 * time.Minute
	scoringTimeout     = 60 * time.Minute
	maxVerticalNameLen = 96
	maxVerticalSlugLen = 72

	// Narrative fields are only last-resort naming fallbacks and should stay concise.
	maxNarrativeFallbackNameLen   = 72
	maxNarrativeFallbackNameWords = 8

	localServicesScannerExpected = 5
	corpusBatchSize              = 25
)

type pipelineEmitCollectorKey struct{}
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

// FactoryPipelineCoordinator handles deterministic factory state-machine work
// (scan assignment/aggregation and validation gate orchestration) so LLM agents
// focus on judgment tasks.
type FactoryPipelineCoordinator struct {
	bus Bus
	db  *sql.DB

	mu sync.Mutex

	scanCoordinator *ScanCoordinator
	scoringState    *ScoringState
	validationGate  *ValidationGate
	module          WorkflowModule
	discoveryPolicy DiscoveryPolicy
	scoringPolicy   ScoringPolicy
	payloads        PayloadFactory
	processed       map[string]struct{}
	stateStore      *PipelineStateStore
	workflowStore   *WorkflowInstanceStore
	stateLoaded     bool

	statePersistenceChecked bool
	statePersistenceEnabled bool

	shardPlanner       *ShardPlanner
	payloadFactory     *PipelinePayloadFactory
	shardsTableChecked bool
	shardsTableEnabled bool

	scoringDigestBufferChecked bool
	scoringDigestBufferEnabled bool
	lastScoringDigestReadAt    time.Time

	testSubscribeHook     func()
	testVerticalStageHook func(verticalID, stage string)
}

type FactoryPipelineCoordinatorOptions struct {
	ShardPlanner *ShardPlanner
	Module       WorkflowModule
}

func NewFactoryPipelineCoordinatorWithOptions(bus Bus, db *sql.DB, opts FactoryPipelineCoordinatorOptions) *FactoryPipelineCoordinator {
	if bus == nil {
		return nil
	}
	module := opts.Module
	if module == nil {
		module = defaultWorkflowModule()
	}
	discoveryPolicy := module.DiscoveryPolicy()
	scoringPolicy := module.ScoringPolicy()
	payloads := module.PayloadFactory()
	pc := &FactoryPipelineCoordinator{
		bus:             bus,
		db:              db,
		scanCoordinator: NewScanCoordinator(),
		scoringState:    NewScoringState(),
		validationGate:  NewValidationGate(),
		module:          module,
		discoveryPolicy: discoveryPolicy,
		scoringPolicy:   scoringPolicy,
		payloads:        payloads,
		processed:       make(map[string]struct{}),
		shardPlanner:    opts.ShardPlanner,
	}
	pc.stateStore = NewPipelineStateStore(db, &pc.mu)
	pc.workflowStore = NewWorkflowInstanceStore(db)
	pc.scanCoordinator.runtime = pc
	pc.scanCoordinator.discovery = discoveryPolicy
	pc.scoringState.runtime = pc
	pc.scoringState.scoring = scoringPolicy
	pc.validationGate.runtime = pc
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

func (pc *FactoryPipelineCoordinator) OnVerticalDiscovered(ctx context.Context, evt events.Event) {
	pc.handleScoringRequested(withPipelineSourceAgent(ctx, ScoringNodeID), evt)
}

func (pc *FactoryPipelineCoordinator) OnVerticalDerived(ctx context.Context, evt events.Event) {
	pc.handleVerticalDerived(withPipelineSourceAgent(ctx, ScoringNodeID), evt)
}

func (pc *FactoryPipelineCoordinator) OnScoreDimensionComplete(ctx context.Context, evt events.Event) {
	pc.handleScoreDimensionComplete(withPipelineSourceAgent(ctx, ScoringNodeID), evt)
}

func (pc *FactoryPipelineCoordinator) OnScoringContestResolved(ctx context.Context, evt events.Event) {
	pc.handleScoringContestResolved(withPipelineSourceAgent(ctx, ScoringNodeID), evt)
}

func (pc *FactoryPipelineCoordinator) SetTestSubscribeHook(fn func()) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.testSubscribeHook = fn
	pc.mu.Unlock()
}

func (pc *FactoryPipelineCoordinator) SetTestVerticalStageHook(fn func(verticalID, stage string)) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.testVerticalStageHook = fn
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
	VerticalID       string
	VerticalName     string
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
	VerticalID string
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

type DedupCandidatePayload struct {
	Name           string  `json:"name,omitempty"`
	Geography      string  `json:"geography,omitempty"`
	SignalStrength float64 `json:"signal_strength,omitempty"`
	ID             string  `json:"id,omitempty"`
}

type VerticalDerivedPayload struct {
	OpportunityID         string         `json:"opportunity_id,omitempty"`
	ParentID              string         `json:"parent_id"`
	GenerationDepth       int            `json:"generation_depth"`
	GeneratorAgentID      string         `json:"generator_agent_id"`
	CampaignID            string         `json:"campaign_id,omitempty"`
	DerivationRationale   map[string]any `json:"derivation_rationale"`
	OpportunityName       string         `json:"opportunity_name"`
	PreliminaryICP        string         `json:"preliminary_icp,omitempty"`
	BuildSketch           map[string]any `json:"build_sketch,omitempty"`
	Evidence              map[string]any `json:"evidence,omitempty"`
	GeographicScope       string         `json:"geographic_scope,omitempty"`
	SignalStrength        float64        `json:"signal_strength"`
	DiscoveryContext      map[string]any `json:"discovery_context,omitempty"`
	OpportunityHypothesis string         `json:"opportunity_hypothesis,omitempty"`
	Geography             string         `json:"geography,omitempty"`
	OpportunityPattern    string         `json:"opportunity_pattern,omitempty"`
	SignalSources         any            `json:"signal_sources,omitempty"`
	RequiredCapabilities  any            `json:"required_capabilities,omitempty"`
}

func NewFactoryPipelineCoordinator(bus Bus, db *sql.DB) *FactoryPipelineCoordinator {
	return NewFactoryPipelineCoordinatorWithOptions(bus, db, FactoryPipelineCoordinatorOptions{Module: defaultWorkflowModule()})
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

func (pc *FactoryPipelineCoordinator) notifyTestVerticalStageUpdated(verticalID, stage string) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	hook := pc.testVerticalStageHook
	pc.mu.Unlock()
	if hook != nil {
		hook(strings.TrimSpace(verticalID), strings.TrimSpace(stage))
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
			pc.checkPackagingTimeouts(ctx, time.Now().UTC())
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

	if eventType == "spec.revision_needed" && strings.TrimSpace(evt.VerticalID) != "" {
		escalated := pc.handleInnerSpecRevision(ctx, evt)
		pc.checkPackagingTimeouts(ctx, time.Now().UTC())
		if escalated {
			record("consumed", "", nil, nil)
		} else {
			record("processed", "", nil, nil)
		}
		return !escalated, nil, nil
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
	pc.checkPackagingTimeouts(ictx, time.Now().UTC())

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
		if strings.TrimSpace(evt.VerticalID) == "" {
			return "missing vertical_id"
		}
	}
	return ""
}

func (pc *FactoryPipelineCoordinator) interceptStateDropReason(eventType string, evt events.Event) string {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return ""
	}

	pc.mu.Lock()
	st := pc.validationGate.states[verticalID]
	pc.mu.Unlock()

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
	return pc.bus.Subscribe("pipeline-coordinator", defaultPipelineSubscriptions()...)
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
		pc.resetInMemoryState()
		pc.clearPersistentState(ctx)
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
