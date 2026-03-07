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
	processed       map[string]struct{}
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
}

func NewFactoryPipelineCoordinatorWithOptions(bus Bus, db *sql.DB, opts FactoryPipelineCoordinatorOptions) *FactoryPipelineCoordinator {
	pc := NewFactoryPipelineCoordinator(bus, db)
	if pc == nil {
		return nil
	}
	pc.shardPlanner = opts.ShardPlanner
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

type scoreDimensionResult struct {
	Score      int    `json:"score"`
	Evidence   string `json:"evidence"`
	Confidence string `json:"confidence,omitempty"`
}

type contestedDimension struct {
	Dimension string                 `json:"dimension"`
	Scores    []int                  `json:"scores"`
	Evidence  []string               `json:"evidence"`
	Spread    int                    `json:"spread"`
	Options   []scoreDimensionResult `json:"options,omitempty"`
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

type ValidationStartedPayload struct {
	VerticalID     string         `json:"vertical_id"`
	VerticalName   string         `json:"vertical_name,omitempty"`
	Name           string         `json:"name,omitempty"`
	Geography      string         `json:"geography,omitempty"`
	ScoringContext string         `json:"scoring_context,omitempty"`
	Scoring        map[string]any `json:"scoring,omitempty"`
}

type BrandRequestedPayload struct {
	VerticalID    string         `json:"vertical_id"`
	VerticalName  string         `json:"vertical_name,omitempty"`
	Name          string         `json:"name,omitempty"`
	Geography     string         `json:"geography,omitempty"`
	Scoring       map[string]any `json:"scoring"`
	BusinessBrief map[string]any `json:"business_brief,omitempty"`
}

type ValidationPackageReadyPayload struct {
	VerticalID   string         `json:"vertical_id"`
	VerticalName string         `json:"vertical_name,omitempty"`
	Geography    string         `json:"geography,omitempty"`
	Research     map[string]any `json:"research"`
	Spec         map[string]any `json:"spec"`
	CTONotes     map[string]any `json:"cto_notes"`
	Brand        map[string]any `json:"brand"`
	Scoring      map[string]any `json:"scoring"`
	SpecVersion  int            `json:"spec_version"`
}

type SpecValidationRequestedPayload struct {
	VerticalID  string         `json:"vertical_id"`
	SpecContent map[string]any `json:"spec_content"`
	SpecTier    string         `json:"spec_tier"`
}

type CTOSpecReviewRequestedPayload struct {
	VerticalID      string         `json:"vertical_id"`
	MvPSpec         string         `json:"mvp_spec,omitempty"`
	BusinessBrief   map[string]any `json:"business_brief,omitempty"`
	VerticalContext map[string]any `json:"vertical_context,omitempty"`
	VerticalName    string         `json:"vertical_name,omitempty"`
	Geography       string         `json:"geography,omitempty"`
	SpecValidation  map[string]any `json:"spec_validation"`
	SpecVersion     int            `json:"spec_version"`
	Research        map[string]any `json:"research"`
	Spec            map[string]any `json:"spec"`
	Scoring         map[string]any `json:"scoring"`
}

type SpecRevisionRequestedPayload struct {
	VerticalID   string         `json:"vertical_id"`
	CTOFeedback  string         `json:"cto_feedback,omitempty"`
	VerticalName string         `json:"vertical_name,omitempty"`
	Geography    string         `json:"geography,omitempty"`
	Source       string         `json:"source"`
	Feedback     map[string]any `json:"feedback"`
	Research     map[string]any `json:"research"`
	Spec         map[string]any `json:"spec"`
	Scoring      map[string]any `json:"scoring"`
}

type ValidationMoreDataNeededPayload struct {
	VerticalID   string         `json:"vertical_id"`
	Questions    string         `json:"questions,omitempty"`
	VerticalName string         `json:"vertical_name,omitempty"`
	Geography    string         `json:"geography,omitempty"`
	Request      map[string]any `json:"request"`
	Research     map[string]any `json:"research"`
	Spec         map[string]any `json:"spec"`
	Scoring      map[string]any `json:"scoring"`
}

type BrandRevisionNeededPayload struct {
	VerticalID   string         `json:"vertical_id"`
	VerticalName string         `json:"vertical_name,omitempty"`
	Geography    string         `json:"geography,omitempty"`
	Feedback     map[string]any `json:"feedback"`
	Brand        map[string]any `json:"brand"`
}

type VerticalKilledPayload struct {
	VerticalID   string         `json:"vertical_id"`
	VerticalName string         `json:"vertical_name,omitempty"`
	Geography    string         `json:"geography,omitempty"`
	SourceEvent  string         `json:"source_event"`
	Priority     string         `json:"priority"`
	Reason       map[string]any `json:"reason"`
}

type ScanAssignedPayload struct {
	ScanID             string `json:"scan_id"`
	CampaignID         string `json:"campaign_id,omitempty"`
	Mode               string `json:"mode,omitempty"`
	Geography          string `json:"geography,omitempty"`
	GeographyID        string `json:"geography_id,omitempty"`
	TaxonomyCategories any    `json:"taxonomy_categories,omitempty"`
	Priority           string `json:"priority,omitempty"`
	CampaignContext    any    `json:"campaign_context,omitempty"`
	DirectiveID        string `json:"directive_id,omitempty"`
	StrategicContext   any    `json:"strategic_context,omitempty"`
	CorpusPath         string `json:"corpus_path,omitempty"`
	CorpusSignals      any    `json:"corpus_signals,omitempty"`
	RequestedAt        string `json:"requested_at,omitempty"`
	PlannedShards      int    `json:"planned_shards,omitempty"`
}

type SynthesisNeededPayload struct {
	ScanID        string         `json:"scan_id"`
	CampaignID    string         `json:"campaign_id,omitempty"`
	Mode          string         `json:"mode,omitempty"`
	Geography     string         `json:"geography,omitempty"`
	Category      string         `json:"category,omitempty"`
	Subcategory   string         `json:"subcategory,omitempty"`
	ConflictNotes any            `json:"conflict_notes,omitempty"`
	RawReport     map[string]any `json:"raw_report,omitempty"`
}

type DedupCandidatePayload struct {
	Name           string  `json:"name,omitempty"`
	Geography      string  `json:"geography,omitempty"`
	SignalStrength float64 `json:"signal_strength,omitempty"`
	ID             string  `json:"id,omitempty"`
}

type DedupAmbiguousPayload struct {
	ScanID           string                `json:"scan_id"`
	DedupID          string                `json:"dedup_id"`
	DedupEventID     string                `json:"dedup_event_id"`
	Similarity       float64               `json:"similarity"`
	NewCandidate     DedupCandidatePayload `json:"new_candidate"`
	ExistingVertical DedupCandidatePayload `json:"existing_vertical"`
}

type VerticalDiscoveredPayload struct {
	VerticalID           string         `json:"vertical_id"`
	VerticalName         string         `json:"vertical_name,omitempty"`
	Name                 string         `json:"name,omitempty"`
	Geography            string         `json:"geography,omitempty"`
	GeographicScope      string         `json:"geographic_scope,omitempty"`
	Mode                 string         `json:"mode,omitempty"`
	ScanID               string         `json:"scan_id,omitempty"`
	CampaignID           string         `json:"campaign_id,omitempty"`
	SignalStrength       float64        `json:"signal_strength,omitempty"`
	OpportunityPattern   string         `json:"opportunity_pattern,omitempty"`
	SignalSources        any            `json:"signal_sources,omitempty"`
	RequiredCapabilities any            `json:"required_capabilities,omitempty"`
	DiscoverySource      string         `json:"discovery_source,omitempty"`
	RawSignals           map[string]any `json:"raw_signals,omitempty"`
	DiscoveryContext     map[string]any `json:"discovery_context,omitempty"`
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

type ScanCompletedPayload struct {
	ScanID          string `json:"scan_id"`
	CampaignID      string `json:"campaign_id,omitempty"`
	Mode            string `json:"mode,omitempty"`
	Geography       string `json:"geography,omitempty"`
	ReportsReceived int    `json:"reports_received"`
	Expected        int    `json:"agents_expected"`
	Complete        int    `json:"agents_complete"`
	Discovered      int    `json:"verticals_discovered"`
	Skipped         int    `json:"verticals_skipped"`
	PendingDedup    int    `json:"pending_dedup"`
	TimedOut        bool   `json:"timed_out"`
	ShardsTotal     int    `json:"shards_total,omitempty"`
	ShardsCompleted int    `json:"shards_completed,omitempty"`
	ShardsFailed    int    `json:"shards_failed,omitempty"`
}

type ScoringRequestedPayload struct {
	VerticalID              string         `json:"vertical_id"`
	VerticalName            string         `json:"vertical_name,omitempty"`
	Geography               string         `json:"geography,omitempty"`
	Mode                    string         `json:"mode,omitempty"`
	Rubric                  string         `json:"rubric,omitempty"`
	DimensionsRequested     []string       `json:"dimensions_requested"`
	DiscoveryContext        map[string]any `json:"discovery_context,omitempty"`
	AssignedAnalysisAgentID string         `json:"assigned_analysis_agent_id,omitempty"`
	ExcludedAnalysisAgentID string         `json:"excluded_analysis_agent_id,omitempty"`
}

type ScoringContestedPayload struct {
	VerticalID string   `json:"vertical_id"`
	Dimension  string   `json:"dimension"`
	Scores     []int    `json:"scores"`
	Evidence   []string `json:"evidence,omitempty"`
	Spread     int      `json:"spread"`
	Rubric     string   `json:"rubric,omitempty"`
	Mode       string   `json:"mode,omitempty"`
}

type VerticalScoredPayload struct {
	VerticalID     string                          `json:"vertical_id"`
	Result         string                          `json:"result,omitempty"`
	Reason         string                          `json:"reason,omitempty"`
	CompositeScore float64                         `json:"composite_score"`
	ViabilityScore float64                         `json:"viability_score"`
	MarketScore    float64                         `json:"market_score"`
	Dimensions     map[string]scoreDimensionResult `json:"dimensions"`
	Rubric         string                          `json:"rubric,omitempty"`
	Partial        bool                            `json:"partial"`
	Mode           string                          `json:"mode,omitempty"`
	VerticalName   string                          `json:"vertical_name,omitempty"`
	Geography      string                          `json:"geography,omitempty"`
}

type VerticalShortlistedPayload struct {
	VerticalID     string         `json:"vertical_id"`
	CompositeScore float64        `json:"composite_score"`
	ViabilityScore float64        `json:"viability_score"`
	ScoringPayload map[string]any `json:"scoring_payload"`
}

type VerticalMarginalPayload struct {
	VerticalID        string                          `json:"vertical_id"`
	CompositeScore    float64                         `json:"composite_score"`
	ViabilityScore    float64                         `json:"viability_score"`
	Dimensions        map[string]scoreDimensionResult `json:"dimensions"`
	PromotionEligible bool                            `json:"promotion_eligible"`
}

type VerticalRejectedPayload struct {
	VerticalID string `json:"vertical_id"`
	Reason     string `json:"reason"`
}

type PortfolioDigestTimerPayload struct {
	Message                   string           `json:"message,omitempty"`
	DigestText                string           `json:"digest_text,omitempty"`
	TriggerReason             string           `json:"trigger_reason,omitempty"`
	Snapshot                  map[string]any   `json:"snapshot,omitempty"`
	Metadata                  map[string]any   `json:"metadata,omitempty"`
	VerticalID                string           `json:"vertical_id,omitempty"`
	TaskID                    string           `json:"task_id,omitempty"`
	RecentRejections          []map[string]any `json:"recent_rejections,omitempty"`
	RejectionCount            int              `json:"rejection_count,omitempty"`
	ScoringRejectionsInjected bool             `json:"scoring_rejections_injected"`
	ScoringRejectionsCount    int              `json:"scoring_rejections_count,omitempty"`
	ScoringRejectionSummaries []map[string]any `json:"scoring_rejection_summaries,omitempty"`
}

type validationContextSnapshot struct {
	Research    map[string]any
	Spec        map[string]any
	CTONotes    map[string]any
	Brand       map[string]any
	Scoring     map[string]any
	SpecVersion int
}

func NewFactoryPipelineCoordinator(bus Bus, db *sql.DB) *FactoryPipelineCoordinator {
	if bus == nil {
		return nil
	}
	pc := &FactoryPipelineCoordinator{
		bus:             bus,
		db:              db,
		scanCoordinator: NewScanCoordinator(),
		scoringState:    NewScoringState(),
		validationGate:  NewValidationGate(),
		processed:       make(map[string]struct{}),
	}
	pc.scanCoordinator.runtime = pc
	pc.scoringState.runtime = pc
	pc.validationGate.runtime = pc
	pc.payloadFactory = NewPipelinePayloadFactory(pc)
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
	return pc.bus.Subscribe("pipeline-coordinator", empirePipelineSubscriptions()...)
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
