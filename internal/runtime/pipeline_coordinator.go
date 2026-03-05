package runtime

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
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
	bus *EventBus
	db  *sql.DB

	mu sync.Mutex

	scans        map[string]*scanAccumulator
	scoring      map[string]*scoringAccumulator
	pendingDedup map[string]pendingCandidate
	validations  map[string]*validationPipelineState
	processed    map[string]struct{}
	stateLoaded  bool

	statePersistenceChecked bool
	statePersistenceEnabled bool

	shardPlanner       *ShardPlanner
	shardsTableChecked bool
	shardsTableEnabled bool

	scoringDigestBufferChecked bool
	scoringDigestBufferEnabled bool
	lastScoringDigestReadAt    time.Time
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
	VerticalID   string         `json:"vertical_id"`
	VerticalName string         `json:"vertical_name,omitempty"`
	Name         string         `json:"name,omitempty"`
	Geography    string         `json:"geography,omitempty"`
	Scoring      map[string]any `json:"scoring"`
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
	VerticalID     string         `json:"vertical_id"`
	VerticalName   string         `json:"vertical_name,omitempty"`
	Geography      string         `json:"geography,omitempty"`
	SpecValidation map[string]any `json:"spec_validation"`
	SpecVersion    int            `json:"spec_version"`
	Research       map[string]any `json:"research"`
	Spec           map[string]any `json:"spec"`
	Scoring        map[string]any `json:"scoring"`
}

type SpecRevisionRequestedPayload struct {
	VerticalID   string         `json:"vertical_id"`
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

func NewFactoryPipelineCoordinator(bus *EventBus, db *sql.DB) *FactoryPipelineCoordinator {
	if bus == nil {
		return nil
	}
	return &FactoryPipelineCoordinator{
		bus:          bus,
		db:           db,
		scans:        make(map[string]*scanAccumulator),
		scoring:      make(map[string]*scoringAccumulator),
		pendingDedup: make(map[string]pendingCandidate),
		validations:  make(map[string]*validationPipelineState),
		processed:    make(map[string]struct{}),
	}
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

	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				pc.resetInMemoryState()
				ch = pc.subscribe()
				continue
			}
			pc.handleEvent(ctx, evt)
		}
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
	st := pc.validations[verticalID]
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
	switch eventType {
	case "timer.portfolio_digest":
		payload := parsePayloadMap(evt.Payload)
		if boolFromAny(payload["scoring_rejections_injected"]) {
			return false, false
		}
		return true, true
	case "vertical.scored":
		payload := parsePayloadMap(evt.Payload)
		result := strings.ToLower(strings.TrimSpace(asString(payload["result"])))
		// Keep vertical.scored as an audit event but avoid waking EC for
		// non-shortlisted outcomes. Marginals route via vertical.marginal.
		switch result {
		case "marginal", "rejected":
			return true, true
		default:
			return false, true
		}
	case "scan.requested",
		"category.assessed",
		"trend.identified",
		"source.scraped",
		"market_research.scan_complete",
		"trend_research.scan_complete",
		"scanner.google_maps.scan_complete",
		"scanner.instagram.scan_complete",
		"scanner.reviews.scan_complete",
		"scanner.directories.scan_complete",
		"scanner.yelp.scan_complete",
		"dedup.resolved",
		"synthesis.resolved",
		"vertical.shortlisted",
		"research.completed",
		"research.vertical_rejected",
		"spec.approved",
		"cto.spec_approved",
		"cto.spec_revision_needed",
		"cto.spec_vetoed",
		"brand.candidates_ready",
		"vertical.needs_more_data",
		"vertical.resumed":
		return true, true
	case "spec.revision_requested", "brand.revision_needed":
		if strings.TrimSpace(evt.VerticalID) == "" {
			return false, false
		}
		// Runtime updates pipeline state, but event must still reach subscribed agents.
		return false, true
	case "spec.validation_passed", "spec.validation_failed":
		if strings.TrimSpace(evt.VerticalID) == "" {
			return false, false
		}
		return true, true
	case "vertical.approved", "vertical.killed":
		if strings.TrimSpace(evt.VerticalID) == "" {
			return false, false
		}
		// Keep event visible for downstream consumers while updating stage projection.
		return false, true
	case "opco.ceo_ready":
		if strings.TrimSpace(evt.VerticalID) == "" {
			return false, false
		}
		// Keep event visible for downstream consumers while updating stage projection.
		return false, true
	case "vertical.ready_for_review":
		if strings.TrimSpace(evt.VerticalID) == "" {
			return false, false
		}
		// Update state but keep event for audit visibility.
		return false, true
	case "runtime.reset":
		return false, true
	default:
		return false, false
	}
}

func (pc *FactoryPipelineCoordinator) subscribe() <-chan events.Event {
	return pc.bus.Subscribe("pipeline-coordinator",
		events.EventType("timer.portfolio_digest"),
		events.EventType("scan.requested"),
		events.EventType("category.assessed"),
		events.EventType("trend.identified"),
		events.EventType("source.scraped"),
		events.EventType("market_research.scan_complete"),
		events.EventType("trend_research.scan_complete"),
		events.EventType("scanner.google_maps.scan_complete"),
		events.EventType("scanner.instagram.scan_complete"),
		events.EventType("scanner.reviews.scan_complete"),
		events.EventType("scanner.directories.scan_complete"),
		events.EventType("scanner.yelp.scan_complete"),
		events.EventType("dedup.resolved"),
		events.EventType("synthesis.resolved"),
		events.EventType("vertical.shortlisted"),
		events.EventType("research.completed"),
		events.EventType("research.vertical_rejected"),
		events.EventType("spec.revision_requested"),
		events.EventType("spec.approved"),
		events.EventType("spec.validation_passed"),
		events.EventType("spec.validation_failed"),
		events.EventType("vertical.approved"),
		events.EventType("vertical.killed"),
		events.EventType("opco.ceo_ready"),
		events.EventType("cto.spec_approved"),
		events.EventType("cto.spec_revision_needed"),
		events.EventType("cto.spec_vetoed"),
		events.EventType("brand.candidates_ready"),
		events.EventType("vertical.ready_for_review"),
		events.EventType("vertical.needs_more_data"),
		events.EventType("brand.revision_needed"),
		events.EventType("vertical.resumed"),
		events.EventType("runtime.reset"),
	)
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
	case "timer.portfolio_digest":
		pc.handlePortfolioDigestTimer(ctx, evt)
		return
	case "scan.requested":
		pc.handleScanRequested(ctx, evt)
	case "vertical.scored":
		// Delivery filtering for this event type is handled in interceptPolicy.
		// Keep a no-op case for explicit coverage/traceability in switch audits.
		return
	case "category.assessed", "trend.identified", "source.scraped":
		pc.handleDiscoveryReport(ctx, evt)
	case "market_research.scan_complete", "trend_research.scan_complete",
		"scanner.google_maps.scan_complete", "scanner.instagram.scan_complete",
		"scanner.reviews.scan_complete", "scanner.directories.scan_complete",
		"scanner.yelp.scan_complete":
		pc.handleScanCompletion(ctx, evt)
	case "dedup.resolved":
		pc.handleDedupResolved(ctx, evt)
	case "synthesis.resolved":
		// synthesis is a pure judgment refinement; discovery accumulation
		// already consumed raw reports and does not need additional state here.
		return
	case "vertical.shortlisted":
		pc.handleValidationStarted(ctx, evt)
	case "research.completed":
		pc.handleValidationGate(ctx, evt, "g1")
	case "spec.revision_requested":
		pc.handleSpecRevisionRequested(evt)
	case "spec.revision_needed":
		_ = pc.handleInnerSpecRevision(ctx, evt)
	case "spec.approved":
		pc.handleValidationGate(ctx, evt, "g2")
	case "cto.spec_approved":
		pc.handleCTOApproved(ctx, evt)
	case "brand.candidates_ready":
		pc.handleValidationGate(ctx, evt, "g4")
	case "spec.validation_passed":
		pc.handleSpecValidationPassed(ctx, evt)
	case "spec.validation_failed":
		pc.handleSpecValidationFailed(ctx, evt)
	case "vertical.approved":
		pc.handleVerticalApproved(ctx, evt)
	case "vertical.killed":
		pc.handleVerticalKilled(ctx, evt)
	case "opco.ceo_ready":
		pc.handleOpCoCEOReady(ctx, evt)
	case "cto.spec_revision_needed":
		pc.handleCTORevisionNeeded(ctx, evt)
	case "research.vertical_rejected", "cto.spec_vetoed":
		pc.handleValidationRejected(ctx, evt)
	case "vertical.ready_for_review":
		pc.handleValidationPackaged(ctx, evt)
	case "vertical.needs_more_data":
		pc.handleValidationMoreData(ctx, evt)
	case "brand.revision_needed":
		pc.handleBrandRevision(ctx, evt)
	case "vertical.resumed":
		pc.handleVerticalResumed(ctx, evt)
	}
}

func (pc *FactoryPipelineCoordinator) resetInMemoryState() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.scans = make(map[string]*scanAccumulator)
	pc.scoring = make(map[string]*scoringAccumulator)
	pc.pendingDedup = make(map[string]pendingCandidate)
	pc.validations = make(map[string]*validationPipelineState)
	pc.processed = make(map[string]struct{})
	pc.stateLoaded = true
}

func (pc *FactoryPipelineCoordinator) ensureStateLoaded(ctx context.Context) {
	if pc == nil || pc.db == nil {
		return
	}
	if !pc.isStatePersistenceEnabled(ctx) {
		pc.mu.Lock()
		pc.stateLoaded = true
		pc.mu.Unlock()
		return
	}
	pc.mu.Lock()
	loaded := pc.stateLoaded
	pc.mu.Unlock()
	if loaded {
		return
	}

	scans := make(map[string]*scanAccumulator)
	pending := make(map[string]pendingCandidate)
	validations := make(map[string]*validationPipelineState)
	processed := make(map[string]struct{})

	scanRows, err := dbQueryContext(ctx, pc.db, `
		SELECT scan_id, COALESCE(campaign_id,''), mode, geography,
		       expected, COALESCE(completed_by, '{}'::jsonb), reports,
		       discovered, skipped, COALESCE(started_at, now())
		FROM scan_accumulators
	`)
	if err == nil {
		for scanRows.Next() {
			var (
				scanID, campaignID, mode, geography    string
				expected, reports, discovered, skipped int
				completedRaw                           []byte
				createdAt                              time.Time
			)
			if scanErr := scanRows.Scan(&scanID, &campaignID, &mode, &geography, &expected, &completedRaw, &reports, &discovered, &skipped, &createdAt); scanErr != nil {
				continue
			}
			completedBy := map[string]struct{}{}
			var completedObj map[string]any
			if err := json.Unmarshal(completedRaw, &completedObj); err == nil && len(completedObj) > 0 {
				for key := range completedObj {
					key = strings.TrimSpace(key)
					if key != "" {
						completedBy[key] = struct{}{}
					}
				}
			} else {
				// Backward-compatible fallback if old state persisted an array.
				var completed []string
				_ = json.Unmarshal(completedRaw, &completed)
				for _, key := range completed {
					key = strings.TrimSpace(key)
					if key != "" {
						completedBy[key] = struct{}{}
					}
				}
			}
			scans[scanID] = &scanAccumulator{
				ScanID:      scanID,
				CampaignID:  campaignID,
				Mode:        mode,
				Geography:   geography,
				Expected:    expected,
				CompletedBy: completedBy,
				ReportData:  make([]map[string]any, 0),
				Reports:     reports,
				Discovered:  discovered,
				Skipped:     skipped,
				CreatedAt:   createdAt,
			}
		}
		_ = scanRows.Close()
	}

	pendingRows, err := dbQueryContext(ctx, pc.db, `
		SELECT
			dedup_event_id,
			COALESCE(existing_id, ''),
			scan_id,
			COALESCE(campaign_id, ''),
			COALESCE(mode, ''),
			signal_strength,
			geography,
			discovery_mode,
			COALESCE(name, ''),
			COALESCE(payload, '{}'::jsonb)
		FROM pending_dedup_candidates
		WHERE status = 'pending'
	`)
	if err == nil {
		for pendingRows.Next() {
			var (
				dedupID, existingID, scanID, campaignID, mode, geography, discoveryMode, name string
				signalFloat                                                                   float64
				payloadRaw                                                                    []byte
			)
			if scanErr := pendingRows.Scan(&dedupID, &existingID, &scanID, &campaignID, &mode, &signalFloat, &geography, &discoveryMode, &name, &payloadRaw); scanErr != nil {
				continue
			}
			payload := parsePayloadMap(payloadRaw)
			candidateName := strings.TrimSpace(name)
			if candidateName == "" {
				candidateName = deriveDiscoveryCandidateName(payload)
			}
			resolvedCampaignID := strings.TrimSpace(campaignID)
			if resolvedCampaignID == "" {
				resolvedCampaignID = strings.TrimSpace(asString(payload["campaign_id"]))
			}
			resolvedMode := normalizeScanMode(firstNonEmpty(mode, discoveryMode))
			if resolvedMode == "" {
				resolvedMode = normalizeScanMode(asString(payload["mode"]))
			}
			pending[dedupID] = pendingCandidate{
				DedupEventID: dedupID,
				ExistingID:   strings.TrimSpace(existingID),
				ScanID:       scanID,
				CampaignID:   resolvedCampaignID,
				Mode:         resolvedMode,
				Geography:    geography,
				Name:         candidateName,
				Signal:       signalFloat,
				Payload:      payload,
			}
		}
		_ = pendingRows.Close()
	}

	validationRows, err := dbQueryContext(ctx, pc.db, `
		SELECT vertical_id::text, status, g1_research, g2_spec, g3_cto, g4_brand,
		       COALESCE(research_payload, '{}'::jsonb), COALESCE(spec_payload, '{}'::jsonb),
		       COALESCE(cto_payload, '{}'::jsonb), COALESCE(brand_payload, '{}'::jsonb),
		       COALESCE(scoring_payload, '{}'::jsonb),
		       revision_count, inner_revision_count, spec_version,
		       packaging_requested, packaging_requested_at, packaging_retries
		FROM validation_pipelines
	`)
	if err == nil {
		for validationRows.Next() {
			var (
				verticalID, status                                                     string
				g1, g2, g3, g4, packagingRequested                                     bool
				researchPayload, specPayload, ctoPayload, brandPayload, scoringPayload []byte
				revisionCount, innerRevisionCount, specVersion, packagingRetries       int
				packagingRequestedAt                                                   sql.NullTime
			)
			if scanErr := validationRows.Scan(
				&verticalID, &status, &g1, &g2, &g3, &g4,
				&researchPayload, &specPayload, &ctoPayload, &brandPayload,
				&scoringPayload,
				&revisionCount, &innerRevisionCount, &specVersion,
				&packagingRequested, &packagingRequestedAt, &packagingRetries,
			); scanErr != nil {
				continue
			}
			var packagingAt *time.Time
			if packagingRequestedAt.Valid {
				t := packagingRequestedAt.Time
				packagingAt = &t
			}
			validations[verticalID] = &validationPipelineState{
				VerticalID:           verticalID,
				Status:               status,
				G1Research:           g1,
				G2Spec:               g2,
				G3CTO:                g3,
				G4Brand:              g4,
				ResearchPayload:      cloneRaw(researchPayload),
				SpecPayload:          cloneRaw(specPayload),
				CTOPayload:           cloneRaw(ctoPayload),
				BrandPayload:         cloneRaw(brandPayload),
				ScoringPayload:       cloneRaw(scoringPayload),
				RevisionCount:        revisionCount,
				InnerRevisionCount:   innerRevisionCount,
				SpecVersion:          specVersion,
				PackagingRequested:   packagingRequested || packagingAt != nil,
				PackagingRequestedAt: packagingAt,
				PackagingRetries:     packagingRetries,
			}
		}
		_ = validationRows.Close()
	}

	processedRows, err := dbQueryContext(ctx, pc.db, `
		SELECT event_id::text
		FROM pipeline_processed_events
		WHERE processed_at >= now() - interval '7 days'
	`)
	if err == nil {
		for processedRows.Next() {
			var eventID string
			if scanErr := processedRows.Scan(&eventID); scanErr != nil {
				continue
			}
			processed[eventID] = struct{}{}
		}
		_ = processedRows.Close()
	}

	pc.mu.Lock()
	if pc.stateLoaded {
		pc.mu.Unlock()
		return
	}
	if len(scans) > 0 {
		pc.scans = scans
	}
	if pc.scoring == nil {
		pc.scoring = make(map[string]*scoringAccumulator)
	}
	if len(pending) > 0 {
		pc.pendingDedup = pending
	}
	if len(validations) > 0 {
		pc.validations = validations
	}
	if len(processed) > 0 {
		pc.processed = processed
	}
	pc.stateLoaded = true
	pc.mu.Unlock()

	// Ensure dashboard-facing stage projection is consistent with recovered validation state.
	for verticalID, st := range validations {
		if st == nil {
			continue
		}
		pc.updateVerticalStage(ctx, verticalID, pc.validationStageForState(st), "")
	}
}

func (pc *FactoryPipelineCoordinator) markEventProcessed(ctx context.Context, eventID string) bool {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false
	}
	pc.mu.Lock()
	if _, ok := pc.processed[eventID]; ok {
		pc.mu.Unlock()
		return false
	}
	pc.mu.Unlock()

	if pc.db != nil && pc.isStatePersistenceEnabled(ctx) {
		res, err := dbExecContext(ctx, pc.db, `
			INSERT INTO pipeline_processed_events (event_id, processed_at)
			VALUES ($1, now())
			ON CONFLICT (event_id) DO NOTHING
		`, eventID)
		if err == nil {
			if n, _ := res.RowsAffected(); n == 0 {
				pc.mu.Lock()
				pc.processed[eventID] = struct{}{}
				pc.mu.Unlock()
				return false
			}
		}
	}

	pc.mu.Lock()
	pc.processed[eventID] = struct{}{}
	pc.mu.Unlock()
	return true
}

func (pc *FactoryPipelineCoordinator) persistRuntimeState(ctx context.Context) {
	if pc == nil || pc.db == nil {
		return
	}
	ctx = withoutSQLTxContext(ctx)
	if !pc.isStatePersistenceEnabled(ctx) {
		return
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()

	_, _ = dbExecContext(ctx, pc.db, `DELETE FROM scan_accumulators`)
	_, _ = dbExecContext(ctx, pc.db, `DELETE FROM pending_dedup_candidates`)
	_, _ = dbExecContext(ctx, pc.db, `DELETE FROM validation_pipelines`)

	for _, acc := range pc.scans {
		if acc == nil {
			continue
		}
		if strings.TrimSpace(acc.CampaignID) == "" {
			continue
		}
		completedBy := make([]string, 0, len(acc.CompletedBy))
		completedByMap := make(map[string]any, len(acc.CompletedBy))
		for key := range acc.CompletedBy {
			key = strings.TrimSpace(key)
			if key != "" {
				completedBy = append(completedBy, key)
				completedByMap[key] = true
			}
		}
		sort.Strings(completedBy)
		startedAt := acc.CreatedAt
		if startedAt.IsZero() {
			startedAt = time.Now()
		}
		timeoutAt := startedAt.Add(scanTimeout)
		pendingCount := 0
		for _, cand := range pc.pendingDedup {
			if cand.ScanID == acc.ScanID {
				pendingCount++
			}
		}
		_, _ = dbExecContext(ctx, pc.db, `
			INSERT INTO scan_accumulators (
				scan_id, campaign_id, mode, geography, expected, complete,
				completed_by, reports, discovered, skipped, pending_dedup,
				timeout_at, started_at, completed_at
			)
			VALUES (
				$1, $2, $3, $4, $5,
				$6, $7::jsonb, $8, $9, $10, $11, $12, $13, NULL
			)
			ON CONFLICT (scan_id) DO UPDATE SET
				campaign_id = EXCLUDED.campaign_id,
				mode = EXCLUDED.mode,
				geography = EXCLUDED.geography,
				expected = EXCLUDED.expected,
				complete = EXCLUDED.complete,
				completed_by = EXCLUDED.completed_by,
				reports = EXCLUDED.reports,
				discovered = EXCLUDED.discovered,
				skipped = EXCLUDED.skipped,
				pending_dedup = EXCLUDED.pending_dedup,
				timeout_at = EXCLUDED.timeout_at,
				started_at = EXCLUDED.started_at
		`, acc.ScanID, acc.CampaignID, acc.Mode, acc.Geography, acc.Expected, len(acc.CompletedBy), string(mustJSON(completedByMap)), maxInt(acc.Reports, len(completedBy)), acc.Discovered, acc.Skipped, pendingCount, timeoutAt, startedAt)
	}

	for _, cand := range pc.pendingDedup {
		dedupEventID := strings.TrimSpace(cand.DedupEventID)
		if dedupEventID == "" {
			dedupEventID = stableUUID(cand.ScanID + ":" + cand.Name + ":" + cand.Geography).String()
		}
		candidateName := strings.TrimSpace(cand.Name)
		if candidateName == "" {
			candidateName = deriveDiscoveryCandidateName(cand.Payload)
		}
		_, _ = dbExecContext(ctx, pc.db, `
			INSERT INTO pending_dedup_candidates (
				dedup_event_id, scan_id, campaign_id, mode, name, geography, discovery_mode, signal_strength, payload, existing_id, status, created_at, resolved_at
			)
			VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, NULLIF($10,''), 'pending', now(), NULL
			)
			ON CONFLICT (dedup_event_id) DO UPDATE SET
				campaign_id = EXCLUDED.campaign_id,
				mode = EXCLUDED.mode,
				name = EXCLUDED.name,
				geography = EXCLUDED.geography,
				discovery_mode = EXCLUDED.discovery_mode,
				signal_strength = EXCLUDED.signal_strength,
				payload = EXCLUDED.payload,
				existing_id = EXCLUDED.existing_id
		`, dedupEventID, cand.ScanID, strings.TrimSpace(cand.CampaignID), strings.TrimSpace(cand.Mode), candidateName, cand.Geography, cand.Mode, cand.Signal, string(mustJSON(cand.Payload)), strings.TrimSpace(cand.ExistingID))
	}

	for _, st := range pc.validations {
		if st == nil {
			continue
		}
		var packagingAt any
		if st.PackagingRequestedAt != nil {
			packagingAt = *st.PackagingRequestedAt
		}
		_, _ = dbExecContext(ctx, pc.db, `
			INSERT INTO validation_pipelines (
				vertical_id, status, g1_research, g2_spec, g3_cto, g4_brand,
				research_payload, spec_payload, cto_payload, brand_payload,
				scoring_payload,
				revision_count, inner_revision_count, spec_version,
				packaging_requested, packaging_requested_at, packaging_retries, updated_at
			)
			VALUES (
				$1::uuid, $2, $3, $4, $5, $6,
				$7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb,
				$12, $13, $14, $15, $16, $17, now()
			)
			ON CONFLICT (vertical_id) DO UPDATE SET
				status = EXCLUDED.status,
				g1_research = EXCLUDED.g1_research,
				g2_spec = EXCLUDED.g2_spec,
				g3_cto = EXCLUDED.g3_cto,
				g4_brand = EXCLUDED.g4_brand,
				research_payload = EXCLUDED.research_payload,
				spec_payload = EXCLUDED.spec_payload,
				cto_payload = EXCLUDED.cto_payload,
				brand_payload = EXCLUDED.brand_payload,
				scoring_payload = EXCLUDED.scoring_payload,
				revision_count = EXCLUDED.revision_count,
				inner_revision_count = EXCLUDED.inner_revision_count,
				spec_version = EXCLUDED.spec_version,
				packaging_requested = EXCLUDED.packaging_requested,
				packaging_requested_at = EXCLUDED.packaging_requested_at,
				packaging_retries = EXCLUDED.packaging_retries,
				updated_at = now()
		`,
			st.VerticalID, st.Status, st.G1Research, st.G2Spec, st.G3CTO, st.G4Brand,
			string(mustJSON(parsePayloadMap(st.ResearchPayload))),
			string(mustJSON(parsePayloadMap(st.SpecPayload))),
			string(mustJSON(parsePayloadMap(st.CTOPayload))),
			string(mustJSON(parsePayloadMap(st.BrandPayload))),
			string(mustJSON(parsePayloadMap(st.ScoringPayload))),
			st.RevisionCount, st.InnerRevisionCount, st.SpecVersion,
			st.PackagingRequested, packagingAt, st.PackagingRetries,
		)
	}
}

func (pc *FactoryPipelineCoordinator) clearPersistentState(ctx context.Context) {
	if pc == nil || pc.db == nil {
		return
	}
	ctx = withoutSQLTxContext(ctx)
	if pc.isScoringDigestBufferEnabled(ctx) {
		_, _ = dbExecContext(ctx, pc.db, `DELETE FROM scoring_digest_buffer`)
	}
	if !pc.isStatePersistenceEnabled(ctx) {
		return
	}
	_, _ = dbExecContext(ctx, pc.db, `DELETE FROM scan_accumulators`)
	_, _ = dbExecContext(ctx, pc.db, `DELETE FROM pending_dedup_candidates`)
	_, _ = dbExecContext(ctx, pc.db, `DELETE FROM validation_pipelines`)
	_, _ = dbExecContext(ctx, pc.db, `DELETE FROM pipeline_processed_events`)
}

func (pc *FactoryPipelineCoordinator) isStatePersistenceEnabled(ctx context.Context) bool {
	if pc == nil || pc.db == nil {
		return false
	}
	pc.mu.Lock()
	if pc.statePersistenceChecked {
		enabled := pc.statePersistenceEnabled
		pc.mu.Unlock()
		return enabled
	}
	pc.mu.Unlock()

	var (
		scansOK       bool
		pendingOK     bool
		validationsOK bool
		processedOK   bool
	)
	_ = pc.db.QueryRowContext(ctx, `SELECT to_regclass('public.scan_accumulators') IS NOT NULL`).Scan(&scansOK)
	_ = pc.db.QueryRowContext(ctx, `SELECT to_regclass('public.pending_dedup_candidates') IS NOT NULL`).Scan(&pendingOK)
	_ = pc.db.QueryRowContext(ctx, `SELECT to_regclass('public.validation_pipelines') IS NOT NULL`).Scan(&validationsOK)
	_ = pc.db.QueryRowContext(ctx, `SELECT to_regclass('public.pipeline_processed_events') IS NOT NULL`).Scan(&processedOK)
	enabled := scansOK && pendingOK && validationsOK && processedOK

	pc.mu.Lock()
	defer pc.mu.Unlock()
	if !pc.statePersistenceChecked {
		pc.statePersistenceEnabled = enabled
		pc.statePersistenceChecked = true
	}
	return pc.statePersistenceEnabled
}

func (pc *FactoryPipelineCoordinator) handleScanRequested(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	scanID := strings.TrimSpace(asString(payload["scan_id"]))
	if scanID == "" {
		scanID = evt.ID
	}
	mode := normalizeScanMode(asString(payload["mode"]))
	if mode == "" {
		mode = "saas_gap"
	}
	campaignID := strings.TrimSpace(asString(payload["campaign_id"]))
	if campaignID == "" {
		// v2.0.35 canonical schema requires non-null campaign_id in scan_accumulators.
		// When legacy events omit campaign_id, use scan_id as a stable surrogate.
		campaignID = scanID
	}
	geography := strings.TrimSpace(asString(payload["geography"]))
	if geography == "" {
		geography = strings.TrimSpace(asString(payload["geography_label"]))
	}
	if geography == "" {
		geography = strings.TrimSpace(asString(payload["geography_id"]))
	}

	plannedShardCount := pc.planAndPersistShards(ctx, evt, scanID, mode, payload)

	acc := &scanAccumulator{
		ScanID:      scanID,
		CampaignID:  campaignID,
		Mode:        mode,
		Geography:   geography,
		Expected:    expectedAgents(mode),
		CompletedBy: make(map[string]struct{}),
		ReportData:  make([]map[string]any, 0),
		CreatedAt:   time.Now(),
	}
	if plannedShardCount > 0 {
		acc.Expected = plannedShardCount
	}
	pc.mu.Lock()
	pc.scans[scanID] = acc
	pc.mu.Unlock()

	assigned := pc.buildScanAssignedPayload(scanID, campaignID, mode, geography, payload, plannedShardCount)
	if plannedShardCount > 0 && (mode == "saas_gap" || mode == "saas_trend") {
		// Assignment dispatch is owned by the shard dispatcher loop.
		return
	}

	switch mode {
	case "saas_gap":
		pc.publish(ctx, "market_research.scan_assigned", "", payloadMap(assigned))
	case "saas_trend":
		pc.publish(ctx, "trend_research.scan_assigned", "", payloadMap(assigned))
	case "corpus":
		corpusPath := strings.TrimSpace(asString(payload["corpus_path"]))
		assigned.CorpusPath = corpusPath
		batches, err := readJSONLFile(corpusPath, corpusBatchSize)
		if err != nil {
			runtimeWarn("pipeline-coordinator", "corpus mode read failed path=%q err=%v", corpusPath, err)
			assigned.CorpusSignals = []map[string]any{}
			pc.publish(ctx, "market_research.scan_assigned", "", payloadMap(assigned))
			return
		}
		if len(batches) == 0 {
			assigned.CorpusSignals = []map[string]any{}
			pc.publish(ctx, "market_research.scan_assigned", "", payloadMap(assigned))
			return
		}
		for _, batch := range batches {
			perBatch := assigned
			perBatch.CorpusSignals = batch
			pc.publish(ctx, "market_research.scan_assigned", "", payloadMap(perBatch))
		}
	case "local_services":
		pc.publish(ctx, "scanner.google_maps.scan_assigned", "", payloadMap(assigned))
		pc.publish(ctx, "scanner.instagram.scan_assigned", "", payloadMap(assigned))
		pc.publish(ctx, "scanner.reviews.scan_assigned", "", payloadMap(assigned))
		pc.publish(ctx, "scanner.directories.scan_assigned", "", payloadMap(assigned))
		pc.publish(ctx, "scanner.yelp.scan_assigned", "", payloadMap(assigned))
	default:
		pc.publish(ctx, "market_research.scan_assigned", "", payloadMap(assigned))
	}
}

func (pc *FactoryPipelineCoordinator) planAndPersistShards(
	ctx context.Context,
	evt events.Event,
	scanID, mode string,
	payload map[string]any,
) int {
	if pc == nil || pc.db == nil || evt.ID == "" {
		return 0
	}
	pc.mu.Lock()
	planner := pc.shardPlanner
	pc.mu.Unlock()
	if planner == nil {
		return 0
	}
	if !pc.isShardsTableEnabled(ctx) {
		return 0
	}
	stage := shardStageForScanMode(mode)
	if stage == "" {
		return 0
	}
	assignments, err := planner.Plan(stage, payload)
	if err != nil || len(assignments) == 0 {
		return 0
	}

	rootTaskID := stableUUID(evt.ID)
	scanUUID := stableUUID(scanID)
	now := time.Now().UTC()
	for _, assignment := range assignments {
		deadline := now.Add(assignment.Timeout)
		if assignment.Timeout <= 0 {
			deadline = now.Add(30 * time.Minute)
		}
		shardID := uuid.NewSHA1(rootTaskID, []byte(assignment.Stage+":"+assignment.ShardKey)).String()
		scope := assignment.Scope
		if scope == nil {
			scope = map[string]any{}
		}
		scope["scan_id"] = scanID
		scope["mode"] = mode
		if v := strings.TrimSpace(asString(payload["campaign_id"])); v != "" {
			scope["campaign_id"] = v
		}
		if v := strings.TrimSpace(asString(payload["geography"])); v != "" {
			scope["geography"] = v
		}
		if v := strings.TrimSpace(asString(payload["geography_id"])); v != "" {
			scope["geography_id"] = v
		}
		if v := strings.TrimSpace(asString(payload["priority"])); v != "" {
			scope["priority"] = v
		}
		if v := strings.TrimSpace(asString(payload["directive_id"])); v != "" {
			scope["directive_id"] = v
		}
		if campaignContext := payload["campaign_context"]; campaignContext != nil {
			scope["campaign_context"] = campaignContext
		}
		if strategicContext := payload["strategic_context"]; strategicContext != nil {
			scope["strategic_context"] = strategicContext
		}
		if _, err := dbExecContext(ctx, pc.db, `
			INSERT INTO shards (
				id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
				scope, status, deadline_at, budget_cents, created_at
			)
			VALUES (
				$1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7,
				$8::jsonb, 'pending', $9, $10, now()
			)
			ON CONFLICT (root_task_id, shard_key) DO NOTHING
		`,
			shardID,
			rootTaskID.String(),
			scanUUID.String(),
			assignment.Stage,
			assignment.ShardIndex,
			assignment.ShardCount,
			assignment.ShardKey,
			string(mustJSON(scope)),
			deadline,
			assignment.BudgetCents,
		); err != nil {
			log.Printf("pipeline: shard persist failed scan=%s stage=%s key=%s err=%v", scanID, stage, assignment.ShardKey, err)
			return 0
		}
	}

	var count int
	if err := dbQueryRowContext(ctx, pc.db, `
		SELECT COUNT(*)
		FROM shards
		WHERE root_task_id = $1::uuid
	`, rootTaskID.String()).Scan(&count); err != nil {
		log.Printf("pipeline: shard count failed scan=%s stage=%s err=%v", scanID, stage, err)
		return len(assignments)
	}
	return count
}

func (pc *FactoryPipelineCoordinator) isShardsTableEnabled(ctx context.Context) bool {
	if pc == nil || pc.db == nil {
		return false
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.shardsTableChecked {
		return pc.shardsTableEnabled
	}
	var ok bool
	_ = dbQueryRowContext(ctx, pc.db, `SELECT to_regclass('public.shards') IS NOT NULL`).Scan(&ok)
	pc.shardsTableChecked = true
	pc.shardsTableEnabled = ok
	return pc.shardsTableEnabled
}

func shardStageForScanMode(mode string) string {
	switch normalizeScanMode(mode) {
	case "saas_gap":
		return ShardStageMarketResearch
	case "saas_trend":
		return ShardStageTrendResearch
	default:
		return ""
	}
}

func stableUUID(raw string) uuid.UUID {
	raw = strings.TrimSpace(raw)
	if parsed, err := uuid.Parse(raw); err == nil {
		return parsed
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(raw))
}

func readJSONLFile(path string, batchSize int) ([][]map[string]any, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("corpus_path is required for corpus mode")
	}
	if batchSize <= 0 {
		batchSize = corpusBatchSize
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := make([][]map[string]any, 0, 8)
	current := make([]map[string]any, 0, batchSize)
	sc := bufio.NewScanner(f)
	// Allow reasonably large lines for corpus entries.
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 2*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		row := map[string]any{}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("invalid jsonl row: %w", err)
		}
		current = append(current, row)
		if len(current) >= batchSize {
			out = append(out, current)
			current = make([]map[string]any, 0, batchSize)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(current) > 0 {
		out = append(out, current)
	}
	return out, nil
}

func (pc *FactoryPipelineCoordinator) handleDiscoveryReport(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	scanID := strings.TrimSpace(asString(payload["scan_id"]))
	if scanID == "" {
		runtimeWarn(
			"pipeline-coordinator",
			"dropping discovery report missing scan_id event_id=%s type=%s source=%s",
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(string(evt.Type)),
			strings.TrimSpace(evt.SourceAgent),
		)
		return
	}

	pc.mu.Lock()
	acc := pc.scans[scanID]
	if acc == nil {
		acc = &scanAccumulator{
			ScanID:      scanID,
			Mode:        normalizeScanMode(asString(payload["mode"])),
			Geography:   strings.TrimSpace(asString(payload["geography"])),
			Expected:    1,
			CompletedBy: make(map[string]struct{}),
			ReportData:  make([]map[string]any, 0),
			CreatedAt:   time.Now(),
		}
		if acc.Mode == "" {
			acc.Mode = "saas_gap"
		}
		pc.scans[scanID] = acc
	}
	acc.ReportData = append(acc.ReportData, cloneMap(payload))
	acc.Reports++
	pc.mu.Unlock()

	if payloadIndicatesSynthesisNeeded(payload) {
		pc.publish(ctx, "synthesis.needed", "", payloadMap(pc.buildSynthesisNeededPayload(scanID, acc, payload)))
		return
	}

	candidates := buildDiscoveryCandidatesForReport(acc.Mode, payload)
	for _, cand := range candidates {
		pc.processDiscoveryCandidate(ctx, evt, scanID, acc, cand)
	}
}

type discoveryCandidate struct {
	Mode    string
	Signal  float64
	Payload map[string]any
}

func buildDiscoveryCandidatesForReport(scanMode string, payload map[string]any) []discoveryCandidate {
	baseMode := normalizeScanMode(firstNonEmptyString(asString(payload["mode"]), scanMode))
	if baseMode == "" {
		baseMode = "saas_gap"
	}
	basePayload := cloneMap(payload)
	basePayload["mode"] = baseMode
	candidates := []discoveryCandidate{{
		Mode:    baseMode,
		Signal:  asFloat(basePayload["signal_strength"]),
		Payload: basePayload,
	}}

	autoRaw, _ := payload["automation_micro"].(map[string]any)
	if len(autoRaw) == 0 {
		return candidates
	}

	autoPayload := cloneMap(payload)
	// Let automation-micro propose its own candidate name from its own hypothesis.
	delete(autoPayload, "vertical_name")
	delete(autoPayload, "name")
	delete(autoPayload, "title")
	autoPayload["mode"] = "automation_micro"
	autoPayload["automation_micro"] = autoRaw
	autoPayload["signal_strength"] = autoRaw["signal_strength"]
	if v := strings.TrimSpace(asString(autoRaw["opportunity_hypothesis"])); v != "" {
		autoPayload["opportunity_hypothesis"] = v
	}
	if v := strings.TrimSpace(asString(autoRaw["evidence"])); v != "" {
		autoPayload["evidence"] = v
	}
	candidates = append(candidates, discoveryCandidate{
		Mode:    "automation_micro",
		Signal:  asFloat(autoRaw["signal_strength"]),
		Payload: autoPayload,
	})
	return candidates
}

var (
	roleTokens = []string{
		"owner", "operator", "manager", "founder", "director", "admin", "coordinator", "lead",
	}
	cohortTokens = []string{
		"clinic", "dental", "restaurant", "salon", "agency", "smb", "small business", "warehouse", "logistics",
	}
	workflowTokens = []string{
		"schedule", "booking", "invoice", "payroll", "lead", "dispatch", "inventory", "compliance", "reporting",
	}
	blockingRedFlagTypes = map[string]struct{}{
		"phone_led_sales":            {},
		"enterprise_procurement":     {},
		"relationship_networking":    {},
		"physical_presence_required": {},
		"support_mode_phone_video":   {},
	}
)

func evaluateDiscoveryPreFilter(payload map[string]any, rawSignal float64) (bool, float64, string) {
	signal := applyRedFlagPenalty(rawSignal, payload)
	if signal < 55 {
		return false, signal, "signal_below_threshold"
	}
	if reason := blockingRedFlagReason(payload); reason != "" {
		return false, signal, reason
	}
	// Backwards-compatible fallback for legacy scanner payloads.
	if !hasStructuredDiscoveryContext(payload) {
		return true, signal, ""
	}
	if !passesICPPositiveCheck(payload) {
		return false, signal, "icp_positive_check_failed"
	}
	if !passesEvidenceCompleteness(payload) {
		return false, signal, "evidence_insufficient"
	}
	if !passesRetentionPrimitive(payload) {
		return false, signal, "no_retention_primitive"
	}
	return true, signal, ""
}

func applyRedFlagPenalty(signal float64, payload map[string]any) float64 {
	flags := extractRedFlagTypes(payload)
	if len(flags) == 0 {
		return signal
	}
	penalized := signal - float64(len(flags)*5)
	if penalized < 0 {
		return 0
	}
	if penalized > 100 {
		return 100
	}
	return penalized
}

func hasBlockingRedFlags(payload map[string]any) bool {
	return blockingRedFlagReason(payload) != ""
}

func blockingRedFlagReason(payload map[string]any) string {
	flags := extractRedFlagTypes(payload)
	flagSet := make(map[string]struct{}, len(flags))
	for _, flag := range flags {
		flagSet[flag] = struct{}{}
		if _, blocked := blockingRedFlagTypes[flag]; blocked {
			return "blocking_red_flag"
		}
	}
	_, hasComplexIntegration := flagSet["complex_integration"]
	_, hasHighFeatureCount := flagSet["high_feature_count"]
	_, hasMultiModule := flagSet["multi_module"]
	if hasComplexIntegration && (hasHighFeatureCount || hasMultiModule) {
		return "co_occurrence_block"
	}
	return ""
}

func extractRedFlagTypes(payload map[string]any) []string {
	if len(payload) == 0 {
		return nil
	}
	buildSketch, _ := asObject(payload["build_sketch"])
	redFlags, _ := asArray(buildSketch["red_flags"])
	out := make([]string, 0, len(redFlags))
	for _, item := range redFlags {
		switch typed := item.(type) {
		case string:
			if v := strings.TrimSpace(typed); v != "" {
				out = append(out, strings.ToLower(v))
			}
		case map[string]any:
			if v := strings.TrimSpace(asString(typed["type"])); v != "" {
				out = append(out, strings.ToLower(v))
			}
		}
	}
	return out
}

func hasStructuredDiscoveryContext(payload map[string]any) bool {
	if len(payload) == 0 {
		return false
	}
	if strings.TrimSpace(asString(payload["opportunity_name"])) != "" {
		return true
	}
	if strings.TrimSpace(asString(payload["preliminary_icp"])) != "" {
		return true
	}
	if buildSketch, ok := asObject(payload["build_sketch"]); ok && len(buildSketch) > 0 {
		return true
	}
	if evidence, ok := asObject(payload["evidence"]); ok && len(evidence) > 0 {
		return true
	}
	return false
}

func passesICPPositiveCheck(payload map[string]any) bool {
	icp := strings.ToLower(strings.TrimSpace(asString(payload["preliminary_icp"])))
	hypothesis := strings.ToLower(strings.TrimSpace(asString(payload["opportunity_hypothesis"])))
	text := strings.TrimSpace(icp + " " + hypothesis)
	if text == "" {
		return false
	}
	hasRole := containsAnyToken(text, roleTokens)
	hasCohort := containsAnyToken(text, cohortTokens)
	hasWorkflow := containsAnyToken(text, workflowTokens)
	if !(hasRole || hasCohort) || !hasWorkflow {
		return false
	}
	evidence, _ := asObject(payload["evidence"])
	communities, _ := asArray(evidence["buyer_communities"])
	for _, item := range communities {
		obj, _ := asObject(item)
		if isURLLike(asString(obj["source_url"])) {
			return true
		}
	}
	return false
}

func passesEvidenceCompleteness(payload map[string]any) bool {
	evidence, ok := asObject(payload["evidence"])
	if !ok || len(evidence) == 0 {
		return false
	}
	competitors, _ := asArray(evidence["competitors"])
	buyerCommunities, _ := asArray(evidence["buyer_communities"])
	painSignals, _ := asArray(evidence["pain_signals"])
	if !hasCompetitorEvidence(competitors) {
		return false
	}
	if !hasSourceURL(buyerCommunities) {
		return false
	}
	if !hasSourceURL(painSignals) {
		return false
	}
	regulatory, _ := asArray(evidence["regulatory"])
	urls := collectEvidenceURLs(competitors, buyerCommunities, painSignals, regulatory)
	if len(urls) < 2 {
		return false
	}
	return true
}

func hasCompetitorEvidence(items []any) bool {
	for _, item := range items {
		obj, _ := asObject(item)
		if strings.TrimSpace(asString(obj["name"])) == "" {
			continue
		}
		if strings.TrimSpace(asString(obj["pricing"])) == "" {
			continue
		}
		if !isURLLike(asString(obj["source_url"])) {
			continue
		}
		return true
	}
	return false
}

func hasSourceURL(items []any) bool {
	for _, item := range items {
		obj, _ := asObject(item)
		if isURLLike(asString(obj["source_url"])) {
			return true
		}
	}
	return false
}

func collectEvidenceURLs(parts ...[]any) map[string]struct{} {
	out := make(map[string]struct{})
	for _, items := range parts {
		for _, item := range items {
			obj, _ := asObject(item)
			raw := strings.TrimSpace(strings.ToLower(asString(obj["source_url"])))
			if isURLLike(raw) {
				out[raw] = struct{}{}
			}
		}
	}
	return out
}

func passesRetentionPrimitive(payload map[string]any) bool {
	return len(extractRetentionPrimitives(payload)) > 0
}

func extractRetentionPrimitives(payload map[string]any) []string {
	keys := []string{
		"recurring_data",
		"workflow_embedding",
		"integration_lock_in",
		"compliance_cadence",
		"team_collaboration",
	}
	out := make(map[string]struct{}, len(keys))
	add := func(key string) {
		token := strings.TrimSpace(strings.ToLower(key))
		if token != "" {
			out[token] = struct{}{}
		}
	}
	for _, key := range keys {
		if parseBool(payload[key]) {
			add(key)
		}
	}
	buildSketch, _ := asObject(payload["build_sketch"])
	for _, key := range keys {
		if parseBool(buildSketch[key]) {
			add(key)
		}
	}
	checkArray := func(v any) {
		items, _ := asArray(v)
		for _, item := range items {
			token := strings.TrimSpace(strings.ToLower(asString(item)))
			for _, key := range keys {
				if token == key {
					add(key)
				}
			}
		}
	}
	checkArray(payload["retention_primitives"])
	checkArray(buildSketch["retention_primitives"])

	// v2.0.47 category.assessed schema does not carry explicit retention_primitives.
	// Infer plausible primitives from structured narrative fields to avoid blind drops.
	for _, primitive := range inferRetentionPrimitives(payload) {
		add(primitive)
	}

	result := make([]string, 0, len(out))
	for _, key := range keys {
		if _, ok := out[key]; ok {
			result = append(result, key)
		}
	}
	return result
}

func inferRetentionPrimitives(payload map[string]any) []string {
	textParts := []string{
		strings.ToLower(strings.TrimSpace(asString(payload["opportunity_hypothesis"]))),
		strings.ToLower(strings.TrimSpace(asString(payload["preliminary_icp"]))),
	}
	buildSketch, _ := asObject(payload["build_sketch"])
	if coreFeatures, ok := asArray(buildSketch["core_features"]); ok {
		for _, item := range coreFeatures {
			textParts = append(textParts, strings.ToLower(strings.TrimSpace(asString(item))))
		}
	}
	if integrations, ok := asArray(buildSketch["key_integrations"]); ok {
		for _, item := range integrations {
			textParts = append(textParts, strings.ToLower(strings.TrimSpace(asString(item))))
		}
	}
	requiredCaps, _ := asObject(payload["required_capabilities"])
	if current, ok := asArray(requiredCaps["current"]); ok {
		for _, item := range current {
			textParts = append(textParts, strings.ToLower(strings.TrimSpace(asString(item))))
		}
	}
	textParts = append(textParts, strings.ToLower(strings.TrimSpace(asString(requiredCaps["would_unlock"]))))
	joined := strings.Join(textParts, " ")
	if joined == "" {
		return nil
	}
	out := make(map[string]struct{}, 5)
	add := func(primitive string) { out[primitive] = struct{}{} }
	if containsAnyToken(joined, []string{"calendar", "history", "ledger", "records", "tracking", "dashboard", "library", "portfolio", "reconciliation", "audit trail"}) {
		add("recurring_data")
	}
	if containsAnyToken(joined, []string{"workflow", "approval", "queue", "submission", "tracker", "daily", "weekly", "coordinator"}) {
		add("workflow_embedding")
	}
	if containsAnyToken(joined, []string{"integration", "sync", "api", "oauth", "quickbooks", "xero", "sage", "procore", "clio", "mri", "yardi", "erp"}) {
		add("integration_lock_in")
	}
	if containsAnyToken(joined, []string{"compliance", "deadline", "regulatory", "renewal", "expiration", "guideline", "ocg", "lien waiver", "coi"}) {
		add("compliance_cadence")
	}
	if containsAnyToken(joined, []string{"team", "partner", "manager", "attorney", "coordinator", "approval routing"}) {
		add("team_collaboration")
	}
	keys := []string{"recurring_data", "workflow_embedding", "integration_lock_in", "compliance_cadence", "team_collaboration"}
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		if _, ok := out[key]; ok {
			result = append(result, key)
		}
	}
	return result
}

func buildPrefilterSkipDetail(payload map[string]any, rawSignal, adjustedSignal float64, reason, mode string) map[string]any {
	evidence, _ := asObject(payload["evidence"])
	competitors, _ := asArray(evidence["competitors"])
	buyerCommunities, _ := asArray(evidence["buyer_communities"])
	painSignals, _ := asArray(evidence["pain_signals"])
	regulatory, _ := asArray(evidence["regulatory"])
	evidenceURLs := collectEvidenceURLs(competitors, buyerCommunities, painSignals, regulatory)
	urls := make([]string, 0, len(evidenceURLs))
	for url := range evidenceURLs {
		urls = append(urls, url)
	}
	sort.Strings(urls)
	retentionPrimitives := extractRetentionPrimitives(payload)
	detail := map[string]any{
		"skip_reason":             strings.TrimSpace(reason),
		"mode":                    strings.TrimSpace(mode),
		"raw_signal_strength":     rawSignal,
		"signal_strength":         adjustedSignal,
		"red_flags":               extractRedFlagTypes(payload),
		"evidence_urls":           urls,
		"retention_primitive":     retentionPrimitives,
		"opportunity_name":        strings.TrimSpace(asString(payload["opportunity_name"])),
		"opportunity_pattern":     strings.TrimSpace(asString(payload["opportunity_pattern"])),
		"passes_icp_gate":         passesICPPositiveCheck(payload),
		"passes_evidence_gate":    passesEvidenceCompleteness(payload),
		"passes_retention_gate":   len(retentionPrimitives) > 0,
		"structured_context":      hasStructuredDiscoveryContext(payload),
		"blocking_red_flags_gate": hasBlockingRedFlags(payload),
	}
	return detail
}

func (pc *FactoryPipelineCoordinator) logPrefilterSkip(ctx context.Context, evt events.Event, scanID, campaignID, reason, mode string, payload map[string]any, rawSignal, adjustedSignal float64) {
	if pc == nil || pc.bus == nil {
		return
	}
	pc.bus.logRuntime(ctx, RuntimeLogEntry{
		Level:      "warn",
		Component:  "prefilter",
		Action:     "skipped",
		EventID:    strings.TrimSpace(evt.ID),
		EventType:  strings.TrimSpace(string(evt.Type)),
		AgentID:    strings.TrimSpace(evt.SourceAgent),
		CampaignID: strings.TrimSpace(campaignID),
		ScanID:     strings.TrimSpace(scanID),
		Detail:     buildPrefilterSkipDetail(payload, rawSignal, adjustedSignal, reason, mode),
	})
}

func containsAnyToken(text string, tokens []string) bool {
	for _, token := range tokens {
		tok := strings.TrimSpace(strings.ToLower(token))
		if tok != "" && strings.Contains(text, tok) {
			return true
		}
	}
	return false
}

func isURLLike(raw string) bool {
	s := strings.TrimSpace(strings.ToLower(raw))
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (pc *FactoryPipelineCoordinator) processDiscoveryCandidate(
	ctx context.Context,
	evt events.Event,
	scanID string,
	acc *scanAccumulator,
	candidate discoveryCandidate,
) {
	signal := candidate.Signal
	allowed, adjustedSignal, reason := evaluateDiscoveryPreFilter(candidate.Payload, signal)
	if !allowed {
		pc.logPrefilterSkip(ctx, evt, scanID, acc.CampaignID, reason, candidate.Mode, candidate.Payload, signal, adjustedSignal)
		pc.mu.Lock()
		acc.Skipped++
		pc.mu.Unlock()
		return
	}
	signal = adjustedSignal
	candidate.Payload["signal_strength"] = adjustedSignal

	payload := candidate.Payload
	name := deriveDiscoveryCandidateName(payload)
	if name == "" {
		runtimeWarn(
			"pipeline-coordinator",
			"skipping discovery candidate with missing name scan_id=%s event_id=%s source=%s mode=%s",
			scanID,
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(evt.SourceAgent),
			candidate.Mode,
		)
		pc.mu.Lock()
		acc.Skipped++
		pc.mu.Unlock()
		return
	}

	geography := strings.TrimSpace(firstNonEmptyString(asString(payload["geography"]), acc.Geography))
	if geography == "" {
		geography = "unknown"
	}

	existing, err := pc.loadVerticalsByGeography(ctx, geography)
	if err != nil {
		log.Printf("pipeline: dedup lookup failed scan=%s geo=%s err=%v", scanID, geography, err)
		existing = nil
	}
	for _, v := range existing {
		if normalizeName(v.Name) == normalizeName(name) {
			pc.mu.Lock()
			acc.Skipped++
			pc.mu.Unlock()
			return
		}
	}

	if best, score := fuzzyBestMatch(name, existing); best.ID != "" && score >= 0.70 {
		dedupEventID := uuid.NewString()
		cand := pendingCandidate{
			DedupEventID: dedupEventID,
			ExistingID:   strings.TrimSpace(best.ID),
			ScanID:       scanID,
			CampaignID:   acc.CampaignID,
			Mode:         candidate.Mode,
			Geography:    geography,
			Name:         name,
			Signal:       signal,
			Payload:      payload,
		}
		pc.mu.Lock()
		pc.pendingDedup[dedupEventID] = cand
		pc.mu.Unlock()
		pc.publish(ctx, "dedup.ambiguous", "", payloadMap(pc.buildDedupAmbiguousPayload(scanID, dedupEventID, score, name, geography, signal, best.ID, best.Name)))
		return
	}

	verticalID, err := pc.ensureVerticalDiscovered(ctx, name, geography, candidate.Mode, payload)
	if err != nil {
		log.Printf("pipeline: ensure discovered vertical failed name=%s geo=%s mode=%s err=%v", name, geography, candidate.Mode, err)
		return
	}
	pc.mu.Lock()
	acc.Discovered++
	pc.mu.Unlock()
	discoveredPayload := payloadMap(pc.buildVerticalDiscoveredPayload(verticalID, name, geography, candidate.Mode, scanID, acc.CampaignID, signal, evt.SourceAgent, payload))
	pc.publish(ctx, "vertical.discovered", verticalID, discoveredPayload)
}

func (pc *FactoryPipelineCoordinator) handleDedupResolved(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	dedupEventID := strings.TrimSpace(asString(payload["dedup_event_id"]))
	if dedupEventID == "" {
		return
	}

	pc.mu.Lock()
	cand, ok := pc.pendingDedup[dedupEventID]
	if ok {
		delete(pc.pendingDedup, dedupEventID)
	}
	pc.mu.Unlock()
	if !ok {
		return
	}

	action := strings.ToLower(strings.TrimSpace(asString(payload["action"])))
	if action == "merge" {
		pc.mu.Lock()
		if acc := pc.scans[cand.ScanID]; acc != nil {
			acc.Skipped++
		}
		pc.mu.Unlock()
		return
	}

	verticalID, err := pc.ensureVerticalDiscovered(ctx, cand.Name, cand.Geography, cand.Mode, cand.Payload)
	if err != nil {
		log.Printf("pipeline: dedup keep_both insert failed err=%v", err)
		return
	}
	pc.mu.Lock()
	if acc := pc.scans[cand.ScanID]; acc != nil {
		acc.Discovered++
	}
	pc.mu.Unlock()
	discoveredPayload := payloadMap(pc.buildVerticalDiscoveredPayload(verticalID, cand.Name, cand.Geography, cand.Mode, cand.ScanID, cand.CampaignID, cand.Signal, "pipeline-coordinator", cand.Payload))
	pc.publish(ctx, "vertical.discovered", verticalID, discoveredPayload)
}

func (pc *FactoryPipelineCoordinator) handleScanCompletion(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	scanID := strings.TrimSpace(asString(payload["scan_id"]))
	if scanID == "" {
		runtimeWarn(
			"pipeline-coordinator",
			"dropping scan completion missing scan_id event_id=%s type=%s source=%s",
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(string(evt.Type)),
			strings.TrimSpace(evt.SourceAgent),
		)
		return
	}
	completionKey := strings.TrimSpace(evt.SourceAgent)
	if completionKey == "" {
		completionKey = strings.TrimSpace(string(evt.Type))
	}
	// local_services fanout uses one scanner agent role handling multiple scanner
	// event types; completion accounting must key by scanner completion event type.
	if strings.HasPrefix(strings.TrimSpace(string(evt.Type)), "scanner.") &&
		strings.HasSuffix(strings.TrimSpace(string(evt.Type)), ".scan_complete") {
		completionKey = strings.TrimSpace(string(evt.Type))
	}
	if shardID := pc.markShardCompletedByAgent(ctx, strings.TrimSpace(evt.SourceAgent)); shardID != "" {
		completionKey = "shard:" + shardID
	}
	shardTotal, shardCompleted, shardFailed, hasShardProgress := pc.shardTerminalProgress(ctx, scanID)

	pc.mu.Lock()
	acc := pc.scans[scanID]
	if acc == nil {
		pc.mu.Unlock()
		runtimeWarn(
			"pipeline-coordinator",
			"received scan completion for unknown accumulator scan_id=%s event_id=%s source=%s",
			scanID,
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(evt.SourceAgent),
		)
		return
	}
	acc.CompletedBy[completionKey] = struct{}{}
	done := len(acc.CompletedBy) >= maxInt(acc.Expected, 1)
	stats := pc.buildScanCompletedPayload(scanCompletedBuildInput{
		ScanID:          acc.ScanID,
		CampaignID:      acc.CampaignID,
		Mode:            acc.Mode,
		Geography:       acc.Geography,
		ReportsReceived: acc.Reports,
		Expected:        maxInt(acc.Expected, 1),
		Complete:        len(acc.CompletedBy),
		Discovered:      acc.Discovered,
		Skipped:         acc.Skipped,
		PendingDedup:    pc.pendingDedupCountForScan(acc.ScanID),
		TimedOut:        false,
	})
	if hasShardProgress {
		terminal := shardCompleted + shardFailed
		stats.Expected = shardTotal
		stats.Complete = terminal
		stats.ShardsTotal = shardTotal
		stats.ShardsCompleted = shardCompleted
		stats.ShardsFailed = shardFailed
		done = terminal >= shardTotal && shardTotal > 0
	}
	if done {
		delete(pc.scans, scanID)
	}
	pc.mu.Unlock()

	if done {
		pc.publish(ctx, "scan.completed", "", payloadMap(stats))
	}
}

var rubricDimensions = map[string][]string{
	"universal": {
		"build_complexity",
		"automation_completeness",
		"icp_crispness",
		"distribution_leverage",
		"time_to_value",
		"operational_drag",
		"pain_severity",
		"competition_gap",
		"monetization_clarity",
		"retention_architecture",
		"expansion_potential",
	},
}

var rubricWeights = map[string]map[string]float64{
	"universal": {
		"icp_crispness":          0.15,
		"distribution_leverage":  0.15,
		"time_to_value":          0.15,
		"operational_drag":       0.15,
		"pain_severity":          0.10,
		"competition_gap":        0.10,
		"monetization_clarity":   0.10,
		"retention_architecture": 0.05,
		"expansion_potential":    0.05,
	},
}

var tier1Dimensions = map[string][]string{
	"universal": {"icp_crispness", "distribution_leverage", "time_to_value", "operational_drag"},
}

var tier1DimensionFloor = map[string]int{
	"universal": 50,
}

var tier1SubscoreFloor = map[string]float64{
	"universal": 60,
}

type scoringMarginalDrainRule struct {
	MinHighDims   int
	HighThreshold int
}

var marginalDrainRules = map[string]scoringMarginalDrainRule{
	"universal": {
		MinHighDims:   2,
		HighThreshold: 70,
	},
}

type scoringHardGate struct {
	Dimension string
	MinScore  int
	Reason    string
}

var rubricGates = map[string][]scoringHardGate{
	"universal": {
		{
			Dimension: "build_complexity",
			MinScore:  50,
			Reason:    "gate_build_complexity",
		},
		{
			Dimension: "automation_completeness",
			MinScore:  50,
			Reason:    "gate_automation_completeness",
		},
	},
}

func expectedScoringDimensions(rubric string) []string {
	dims := rubricDimensions[strings.TrimSpace(rubric)]
	if len(dims) == 0 {
		dims = rubricDimensions["universal"]
	}
	out := append([]string{}, dims...)
	return out
}

func selectScoringRubric(mode string) string {
	// v2.0.39: all supported modes map to the universal rubric.
	switch normalizeScanMode(mode) {
	case "automation_micro", "local_services", "saas_gap", "saas_trend", "corpus":
		return "universal"
	default:
		// Derived opportunities are scored by the same universal rubric.
		if strings.EqualFold(strings.TrimSpace(mode), "derived") {
			return "universal"
		}
		return "universal"
	}
}

func clampScore100(v int) int {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

func intFromAny(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	case float64:
		return int(t)
	case float32:
		return int(t)
	case json.Number:
		n, _ := t.Int64()
		return int(n)
	default:
		s := strings.TrimSpace(asString(v))
		if s == "" {
			return 0
		}
		num := json.Number(s)
		n, _ := num.Int64()
		return int(n)
	}
}

func boolFromAny(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case int:
		return t != 0
	case int32:
		return t != 0
	case int64:
		return t != 0
	case float32:
		return t != 0
	case float64:
		return t != 0
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return n != 0
		}
		return false
	default:
		s := strings.ToLower(strings.TrimSpace(asString(v)))
		switch s {
		case "1", "true", "yes", "y", "on":
			return true
		default:
			return false
		}
	}
}

func dimensionInSet(set []string, dim string) bool {
	dim = strings.TrimSpace(dim)
	if dim == "" {
		return false
	}
	for _, item := range set {
		if strings.TrimSpace(item) == dim {
			return true
		}
	}
	return false
}

func (pc *FactoryPipelineCoordinator) loadScoringSeed(ctx context.Context, verticalID string) (name, geography, mode string) {
	if pc == nil || pc.db == nil {
		return "", "", ""
	}
	var rawMode string
	_ = dbQueryRowContext(ctx, pc.db, `
		SELECT COALESCE(name,''), COALESCE(geography,''), COALESCE(mode,'')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&name, &geography, &rawMode)
	if strings.TrimSpace(rawMode) == "" {
		rawMode = "saas_gap"
	}
	return strings.TrimSpace(name), strings.TrimSpace(geography), normalizeScanMode(rawMode)
}

func (pc *FactoryPipelineCoordinator) handleScoringRequested(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	verticalID := strings.TrimSpace(firstNonEmptyString(evt.VerticalID, asString(payload["vertical_id"])))
	if verticalID == "" {
		return
	}
	mode := normalizeScanMode(asString(payload["mode"]))
	if mode == "" {
		_, _, dbMode := pc.loadScoringSeed(ctx, verticalID)
		mode = normalizeScanMode(dbMode)
	}
	if mode == "" {
		mode = "saas_gap"
	}
	rubric := selectScoringRubric(mode)
	expected := expectedScoringDimensions(rubric)
	if len(expected) == 0 {
		return
	}

	name := strings.TrimSpace(firstNonEmptyString(asString(payload["name"]), asString(payload["vertical_name"])))
	geography := strings.TrimSpace(asString(payload["geography"]))
	if strings.TrimSpace(name) == "" || strings.TrimSpace(geography) == "" {
		dbName, dbGeo, _ := pc.loadScoringSeed(ctx, verticalID)
		if strings.TrimSpace(name) == "" {
			name = dbName
		}
		if strings.TrimSpace(geography) == "" {
			geography = dbGeo
		}
	}
	if strings.TrimSpace(name) == "" {
		name = "unknown"
	}
	if strings.TrimSpace(geography) == "" {
		geography = "unknown"
	}

	now := time.Now().UTC()
	discoveryContext, _ := asObject(payload["discovery_context"])
	discoveryContext = cloneMap(discoveryContext)
	if len(discoveryContext) == 0 {
		discoveryContext = buildDiscoveryContextPayload(payload)
	}
	geographicScope := normalizeGeographicScope(asString(payload["geographic_scope"]))
	pc.mu.Lock()
	acc := &scoringAccumulator{
		VerticalID:       verticalID,
		VerticalName:     name,
		Geography:        geography,
		GeographicScope:  geographicScope,
		Mode:             mode,
		Rubric:           rubric,
		DiscoveryContext: discoveryContext,
		Expected:         expected,
		Received:         make(map[string]scoreDimensionResult, len(expected)),
		Contested:        make(map[string]contestedDimension),
		RequestedAt:      now,
		LastUpdatedAt:    now,
		ContestNotified:  make(map[string]bool),
	}
	if existing := pc.scoring[verticalID]; existing != nil {
		// Keep existing progress but refresh metadata when discovery details improve.
		acc = existing
		acc.VerticalName = firstNonEmptyString(name, acc.VerticalName)
		acc.Geography = firstNonEmptyString(geography, acc.Geography)
		if strings.TrimSpace(geographicScope) != "" {
			acc.GeographicScope = geographicScope
		}
		acc.Mode = mode
		acc.Rubric = rubric
		if len(discoveryContext) > 0 {
			acc.DiscoveryContext = cloneMap(discoveryContext)
		}
		acc.Expected = expected
		if acc.Received == nil {
			acc.Received = make(map[string]scoreDimensionResult, len(expected))
		}
		if acc.Contested == nil {
			acc.Contested = make(map[string]contestedDimension)
		}
		if acc.ContestNotified == nil {
			acc.ContestNotified = make(map[string]bool)
		}
	}
	pc.scoring[verticalID] = acc
	pc.mu.Unlock()

	scoringPayload := pc.buildScoringRequestedPayload(verticalID, acc)
	if excluded := pc.derivedScoringGeneratorAgent(ctx, acc); excluded != "" {
		scoringPayload.ExcludedAnalysisAgentID = excluded
		if assigned := pc.selectScoringAnalysisRecipient(excluded); assigned != "" {
			scoringPayload.AssignedAnalysisAgentID = assigned
			pc.publishDirect(ctx, "scoring.requested", verticalID, payloadMap(scoringPayload), []string{assigned})
			return
		}
		runtimeWarn(
			"scoring-node",
			"anti-bias fallback: no alternate analysis recipient available excluded_agent=%s vertical_id=%s",
			excluded,
			verticalID,
		)
	}
	pc.publish(ctx, "scoring.requested", verticalID, payloadMap(scoringPayload))
}

func (pc *FactoryPipelineCoordinator) handleVerticalDerived(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	parentID := strings.TrimSpace(asString(payload["parent_id"]))
	if parentID == "" {
		runtimeWarn("scoring-node", "dropping vertical.derived missing parent_id event_id=%s", strings.TrimSpace(evt.ID))
		return
	}
	generationDepth := intFromAny(payload["generation_depth"])
	if generationDepth < 0 {
		generationDepth = 0
	}
	if generationDepth > 2 {
		runtimeWarn("scoring-node", "dropping vertical.derived depth cap exceeded parent=%s depth=%d", parentID, generationDepth)
		return
	}
	if children, err := pc.countDerivedChildren(ctx, parentID); err == nil && children >= 2 {
		runtimeWarn("scoring-node", "dropping vertical.derived branch cap exceeded parent=%s children=%d", parentID, children)
		return
	}

	signal := asFloat(payload["signal_strength"])
	if signal == 0 {
		// Keep compatibility with emit payloads using integer encoding.
		signal = float64(intFromAny(payload["signal_strength"]))
	}
	allowed, adjustedSignal, reason := evaluateDiscoveryPreFilter(payload, signal)
	if !allowed {
		runtimeWarn("scoring-node", "dropping vertical.derived prefilter reject parent=%s reason=%s", parentID, reason)
		return
	}
	payload["signal_strength"] = adjustedSignal

	name := deriveDiscoveryCandidateName(payload)
	if name == "" {
		name = strings.TrimSpace(asString(payload["opportunity_name"]))
	}
	if name == "" {
		runtimeWarn("scoring-node", "dropping vertical.derived missing opportunity_name parent=%s", parentID)
		return
	}
	geography := strings.TrimSpace(asString(payload["geography"]))
	if geography == "" {
		_, geo, err := pc.loadVerticalIdentity(ctx, parentID)
		if err == nil {
			geography = strings.TrimSpace(geo)
		}
	}
	if geography == "" {
		geography = "unknown"
	}
	payload["parent_id"] = parentID
	payload["generation_depth"] = generationDepth
	payload["opportunity_name"] = name
	payload["mode"] = "derived"

	campaignID := strings.TrimSpace(asString(payload["campaign_id"]))
	verticalID, err := pc.ensureVerticalDiscovered(ctx, name, geography, "derived", payload)
	if err != nil {
		log.Printf("scoring-node: ensure derived vertical failed parent=%s name=%s err=%v", parentID, name, err)
		return
	}
	discoveredPayload := payloadMap(pc.buildVerticalDiscoveredPayload(
		verticalID,
		name,
		geography,
		"derived",
		"", // scan_id (not applicable for derivation)
		campaignID,
		adjustedSignal,
		strings.TrimSpace(evt.SourceAgent),
		payload,
	))
	pc.publish(ctx, "vertical.discovered", verticalID, discoveredPayload)
}

func (pc *FactoryPipelineCoordinator) countDerivedChildren(ctx context.Context, parentID string) (int, error) {
	if pc == nil || pc.db == nil || strings.TrimSpace(parentID) == "" {
		return 0, nil
	}
	var count int
	if err := dbQueryRowContext(ctx, pc.db, `
		SELECT COUNT(*)
		FROM verticals
		WHERE parent_id = $1::uuid
	`, parentID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (pc *FactoryPipelineCoordinator) handleScoreDimensionComplete(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	verticalID := strings.TrimSpace(firstNonEmptyString(evt.VerticalID, asString(payload["vertical_id"])))
	if verticalID == "" {
		return
	}
	dim := strings.TrimSpace(asString(payload["dimension"]))
	if dim == "" {
		return
	}
	score := clampScore100(intFromAny(payload["score"]))
	evidence := strings.TrimSpace(asString(payload["evidence"]))
	confidence := strings.TrimSpace(asString(payload["confidence"]))

	pc.mu.Lock()
	acc := pc.scoring[verticalID]
	if acc == nil {
		acc = &scoringAccumulator{
			VerticalID:      verticalID,
			Rubric:          "universal",
			Expected:        expectedScoringDimensions("universal"),
			Received:        map[string]scoreDimensionResult{},
			Contested:       map[string]contestedDimension{},
			ContestNotified: map[string]bool{},
			RequestedAt:     time.Now().UTC(),
		}
		name, geo, mode := pc.loadScoringSeed(ctx, verticalID)
		acc.VerticalName = name
		acc.Geography = geo
		acc.Mode = mode
		if acc.Mode == "" {
			acc.Mode = "saas_gap"
		}
		acc.Rubric = selectScoringRubric(acc.Mode)
		acc.Expected = expectedScoringDimensions(acc.Rubric)
		pc.scoring[verticalID] = acc
	}
	if acc.LastUpdatedAt.IsZero() {
		acc.LastUpdatedAt = time.Now().UTC()
	}
	if acc.Received == nil {
		acc.Received = map[string]scoreDimensionResult{}
	}
	if acc.Contested == nil {
		acc.Contested = map[string]contestedDimension{}
	}
	if acc.ContestNotified == nil {
		acc.ContestNotified = map[string]bool{}
	}
	prev, hadPrev := acc.Received[dim]
	next := scoreDimensionResult{
		Score:      score,
		Evidence:   evidence,
		Confidence: confidence,
	}
	if hadPrev && absInt(prev.Score-score) > 30 {
		contest := contestedDimension{
			Dimension: dim,
			Scores:    []int{prev.Score, score},
			Evidence:  []string{prev.Evidence, evidence},
			Spread:    absInt(prev.Score - score),
			Options:   []scoreDimensionResult{prev, next},
		}
		acc.Contested[dim] = contest
		if !acc.ContestNotified[dim] {
			acc.ContestNotified[dim] = true
			acc.LastUpdatedAt = time.Now().UTC()
			pc.mu.Unlock()
			pc.publish(ctx, "scoring.contested", verticalID, payloadMap(pc.buildScoringContestedPayload(verticalID, dim, contest, acc)))
			return
		}
		pc.mu.Unlock()
		return
	}

	acc.Received[dim] = next
	delete(acc.Contested, dim)
	delete(acc.ContestNotified, dim)
	acc.LastUpdatedAt = time.Now().UTC()
	ready := len(acc.Contested) == 0 && hasAllExpectedDimensions(acc)
	pc.mu.Unlock()

	if ready {
		pc.finalizeScoringAccumulator(ctx, verticalID, false)
	}
}

func (pc *FactoryPipelineCoordinator) handleScoringContestResolved(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	verticalID := strings.TrimSpace(firstNonEmptyString(evt.VerticalID, asString(payload["vertical_id"])))
	dimension := strings.TrimSpace(asString(payload["dimension"]))
	if verticalID == "" || dimension == "" {
		return
	}
	resolved := clampScore100(intFromAny(payload["resolved_score"]))
	reasoning := strings.TrimSpace(asString(payload["reasoning"]))
	pc.mu.Lock()
	acc := pc.scoring[verticalID]
	if acc == nil {
		pc.mu.Unlock()
		return
	}
	if acc.Received == nil {
		acc.Received = map[string]scoreDimensionResult{}
	}
	if acc.Contested == nil {
		acc.Contested = map[string]contestedDimension{}
	}
	acc.Received[dimension] = scoreDimensionResult{
		Score:      resolved,
		Evidence:   reasoning,
		Confidence: "resolved",
	}
	delete(acc.Contested, dimension)
	delete(acc.ContestNotified, dimension)
	acc.LastUpdatedAt = time.Now().UTC()
	ready := len(acc.Contested) == 0 && hasAllExpectedDimensions(acc)
	pc.mu.Unlock()
	if ready {
		pc.finalizeScoringAccumulator(ctx, verticalID, false)
	}
}

func hasAllExpectedDimensions(acc *scoringAccumulator) bool {
	if acc == nil || len(acc.Expected) == 0 {
		return false
	}
	for _, dim := range acc.Expected {
		if _, ok := acc.Received[dim]; !ok {
			return false
		}
	}
	return true
}

type scoringComposite struct {
	Result         string
	Reason         string
	CompositeScore float64
	ViabilityScore float64
	MarketScore    float64
	Dimensions     map[string]scoreDimensionResult
	Rubric         string
	Partial        bool
}

func (pc *FactoryPipelineCoordinator) computeComposite(acc *scoringAccumulator, partial bool) scoringComposite {
	weights := rubricWeights[acc.Rubric]
	if len(weights) == 0 {
		weights = rubricWeights["universal"]
	}
	tier1Set := tier1Dimensions[acc.Rubric]
	if len(tier1Set) == 0 {
		tier1Set = tier1Dimensions["universal"]
	}

	floor := tier1DimensionFloor[acc.Rubric]
	if floor <= 0 {
		floor = tier1DimensionFloor["universal"]
	}
	subscoreFloor := tier1SubscoreFloor[acc.Rubric]
	if subscoreFloor <= 0 {
		subscoreFloor = tier1SubscoreFloor["universal"]
	}
	marginalRule, ok := marginalDrainRules[acc.Rubric]
	if !ok {
		marginalRule = marginalDrainRules["universal"]
	}

	dimensions := make(map[string]scoreDimensionResult, len(acc.Expected))
	composite := 0.0
	compositeWeight := 0.0
	tier1Sum := 0.0
	tier1Weight := 0.0
	marketSum := 0.0
	marketWeight := 0.0

	for _, dim := range acc.Expected {
		res, ok := acc.Received[dim]
		if !ok {
			res = scoreDimensionResult{Score: 0, Evidence: "missing_dimension_timeout"}
		}
		dimensions[dim] = res
		w := weights[dim]
		if w > 0 {
			composite += float64(res.Score) * w
			compositeWeight += w
		}
		if !dimensionInSet(tier1Set, dim) {
			if w > 0 {
				marketSum += float64(res.Score) * w
				marketWeight += w
			}
			continue
		}
		if w > 0 {
			tier1Sum += float64(res.Score) * w
			tier1Weight += w
		}
	}

	viability := 0.0
	if tier1Weight > 0 {
		viability = tier1Sum / tier1Weight
	}
	market := 0.0
	if marketWeight > 0 {
		market = marketSum / marketWeight
	}
	if compositeWeight > 0 {
		composite = composite / compositeWeight
	}

	if gates, ok := rubricGates[acc.Rubric]; ok {
		for _, gate := range gates {
			res, exists := dimensions[gate.Dimension]
			if !exists {
				continue
			}
			if res.Score < gate.MinScore {
				return scoringComposite{
					Result:         "rejected",
					Reason:         gate.Reason,
					CompositeScore: composite,
					ViabilityScore: viability,
					MarketScore:    market,
					Dimensions:     dimensions,
					Rubric:         acc.Rubric,
					Partial:        partial,
				}
			}
		}
	}

	for _, dim := range tier1Set {
		res, exists := dimensions[dim]
		if !exists {
			continue
		}
		if res.Score < floor {
			return scoringComposite{
				Result:         "rejected",
				Reason:         fmt.Sprintf("tier1_dimension_floor_%s", strings.TrimSpace(dim)),
				CompositeScore: composite,
				ViabilityScore: viability,
				MarketScore:    market,
				Dimensions:     dimensions,
				Rubric:         acc.Rubric,
				Partial:        partial,
			}
		}
	}

	if viability < subscoreFloor {
		return scoringComposite{
			Result:         "rejected",
			Reason:         "viability_floor_execution_fit",
			CompositeScore: composite,
			ViabilityScore: viability,
			MarketScore:    market,
			Dimensions:     dimensions,
			Rubric:         acc.Rubric,
			Partial:        partial,
		}
	}

	if composite < 55 {
		return scoringComposite{
			Result:         "rejected",
			Reason:         "composite_below_threshold",
			CompositeScore: composite,
			ViabilityScore: viability,
			MarketScore:    market,
			Dimensions:     dimensions,
			Rubric:         acc.Rubric,
			Partial:        partial,
		}
	}

	out := scoringComposite{
		Result:         "marginal",
		CompositeScore: composite,
		ViabilityScore: viability,
		MarketScore:    market,
		Dimensions:     dimensions,
		Rubric:         acc.Rubric,
		Partial:        partial,
	}
	if composite >= 75 {
		out.Result = "shortlisted"
		return out
	}
	highCount := 0
	for _, dim := range tier1Set {
		res, exists := dimensions[dim]
		if !exists {
			continue
		}
		if res.Score >= marginalRule.HighThreshold {
			highCount++
		}
	}
	if highCount < marginalRule.MinHighDims {
		out.Result = "rejected"
		out.Reason = "marginal_drain"
		return out
	}
	return out
}

func (pc *FactoryPipelineCoordinator) finalizeScoringAccumulator(ctx context.Context, verticalID string, partial bool) {
	pc.mu.Lock()
	acc := pc.scoring[verticalID]
	if acc == nil {
		pc.mu.Unlock()
		return
	}
	if len(acc.Contested) > 0 {
		pc.mu.Unlock()
		return
	}
	if partial && len(acc.Received) == 0 {
		pc.mu.Unlock()
		return
	}
	result := pc.computeComposite(acc, partial || len(acc.Received) < len(acc.Expected))
	delete(pc.scoring, verticalID)
	pc.mu.Unlock()

	scoredPayload := pc.buildVerticalScoredPayload(verticalID, result, acc)
	scoredPayloadMap := payloadMap(scoredPayload)
	pc.publish(ctx, "vertical.scored", verticalID, scoredPayloadMap)

	stage := "marginal_review"
	switch result.Result {
	case "shortlisted":
		stage = "shortlisted"
		pc.publish(ctx, "vertical.shortlisted", verticalID, payloadMap(pc.buildVerticalShortlistedPayload(verticalID, result.CompositeScore, result.ViabilityScore, scoredPayloadMap)))
	case "marginal":
		pc.publish(ctx, "vertical.marginal", verticalID, payloadMap(pc.buildVerticalMarginalPayload(verticalID, result)))
	case "rejected":
		stage = "killed"
		pc.appendScoringDigestBuffer(ctx, scoredPayload)
		pc.publish(ctx, "vertical.rejected", verticalID, payloadMap(pc.buildVerticalRejectedPayload(verticalID, result)))
	}
	if pc.db != nil {
		if _, err := dbExecContext(ctx, pc.db, `
			UPDATE verticals
			SET stage = $2,
			    scores = $3::jsonb,
			    parked_at = CASE
					WHEN $2 = 'marginal_review' THEN COALESCE(parked_at, now())
					ELSE NULL
				END,
			    kill_reason = CASE WHEN $2 = 'killed' THEN NULLIF($4,'') ELSE kill_reason END,
			    updated_at = now()
			WHERE id = $1::uuid
			`, verticalID, stage, string(mustJSON(scoredPayloadMap)), strings.TrimSpace(result.Reason)); err != nil {
			log.Printf("pipeline: update vertical score state failed vertical=%s err=%v", verticalID, err)
		}
	}
}

func (pc *FactoryPipelineCoordinator) handlePortfolioDigestTimer(ctx context.Context, evt events.Event) {
	raw := parsePayloadMap(evt.Payload)
	pc.mu.Lock()
	since := pc.lastScoringDigestReadAt
	pc.mu.Unlock()
	entries, newest := pc.consumeScoringDigestEntries(ctx, 100, since)
	now := time.Now().UTC()
	if !newest.IsZero() {
		now = newest
	}
	pc.mu.Lock()
	pc.lastScoringDigestReadAt = now
	pc.mu.Unlock()

	snapshot, _ := raw["snapshot"].(map[string]any)
	metadata, _ := raw["metadata"].(map[string]any)
	payload := PortfolioDigestTimerPayload{
		Message:                   strings.TrimSpace(asString(raw["message"])),
		DigestText:                strings.TrimSpace(asString(raw["digest_text"])),
		TriggerReason:             strings.TrimSpace(asString(raw["trigger_reason"])),
		Snapshot:                  snapshot,
		Metadata:                  metadata,
		VerticalID:                strings.TrimSpace(asString(raw["vertical_id"])),
		TaskID:                    strings.TrimSpace(asString(raw["task_id"])),
		RecentRejections:          entries,
		RejectionCount:            len(entries),
		ScoringRejectionsInjected: true,
		ScoringRejectionsCount:    len(entries),
		ScoringRejectionSummaries: entries,
	}
	pc.publish(ctx, "timer.portfolio_digest", strings.TrimSpace(evt.VerticalID), payloadMap(payload))
}

type scoringDigestEntry struct {
	ID           string
	VerticalID   string
	VerticalName string
	Geography    string
	Result       string
	Reason       string
	Composite    float64
	Viability    float64
	ScoredAt     time.Time
}

func (pc *FactoryPipelineCoordinator) appendScoringDigestBuffer(ctx context.Context, scored VerticalScoredPayload) {
	if pc == nil || pc.db == nil {
		return
	}
	if !pc.isScoringDigestBufferEnabled(ctx) {
		return
	}
	summary := strings.TrimSpace(buildScoringRejectionSummary(scored))
	if summary == "" {
		summary = "Scoring rejected due to low viability/composite score."
	}
	if _, err := dbExecContext(ctx, pc.db, `
		INSERT INTO scoring_digest_buffer (
			id, vertical_id, vertical_name, geography, composite, viability, result, reason, scored_at
		)
		VALUES (
			$1::uuid, NULLIF($2,'')::uuid, $3, $4, $5, $6, $7, $8, now()
		)
	`,
		uuid.NewString(),
		strings.TrimSpace(scored.VerticalID),
		strings.TrimSpace(coalesce(strings.TrimSpace(scored.VerticalName), strings.TrimSpace(scored.VerticalID))),
		strings.TrimSpace(coalesce(strings.TrimSpace(scored.Geography), "unspecified")),
		scored.CompositeScore,
		scored.ViabilityScore,
		strings.TrimSpace(coalesce(strings.TrimSpace(scored.Result), "rejected")),
		strings.TrimSpace(coalesce(strings.TrimSpace(scored.Reason), summary)),
	); err != nil {
		log.Printf("pipeline: append scoring digest buffer failed vertical=%s err=%v", strings.TrimSpace(scored.VerticalID), err)
	}
}

func buildScoringRejectionSummary(scored VerticalScoredPayload) string {
	name := strings.TrimSpace(scored.VerticalName)
	if name == "" {
		name = strings.TrimSpace(scored.VerticalID)
	}
	geography := strings.TrimSpace(scored.Geography)
	if geography == "" {
		geography = "unspecified"
	}
	reason := strings.TrimSpace(scored.Reason)
	if reason == "" {
		reason = "rejected"
	}
	return fmt.Sprintf(
		"%s (%s) rejected in scoring: reason=%s composite=%.2f viability=%.2f",
		name,
		geography,
		reason,
		scored.CompositeScore,
		scored.ViabilityScore,
	)
}

func (pc *FactoryPipelineCoordinator) consumeScoringDigestEntries(ctx context.Context, limit int, since time.Time) ([]map[string]any, time.Time) {
	if pc == nil || pc.db == nil || limit <= 0 {
		return nil, time.Time{}
	}
	if !pc.isScoringDigestBufferEnabled(ctx) {
		return nil, time.Time{}
	}
	rows, err := dbQueryContext(ctx, pc.db, `
		SELECT
		    b.id::text AS id,
		    b.vertical_id::text AS vertical_id,
		    COALESCE(b.vertical_name,'') AS vertical_name,
		    COALESCE(b.geography,'') AS geography,
		    COALESCE(b.result,'rejected') AS result,
		    COALESCE(b.reason,'') AS reason,
		    COALESCE(b.composite,0)::double precision AS composite,
		    COALESCE(b.viability,0)::double precision AS viability,
		    COALESCE(b.scored_at, now()) AS scored_at
		FROM scoring_digest_buffer b
		WHERE b.scored_at > $1
		ORDER BY b.scored_at ASC
		LIMIT $2
	`, since.UTC(), limit)
	if err != nil {
		log.Printf("pipeline: consume scoring digest buffer query failed err=%v", err)
		return nil, time.Time{}
	}
	defer rows.Close()

	out := make([]map[string]any, 0, limit)
	var newest time.Time
	for rows.Next() {
		var rec scoringDigestEntry
		if scanErr := rows.Scan(
			&rec.ID,
			&rec.VerticalID,
			&rec.VerticalName,
			&rec.Geography,
			&rec.Result,
			&rec.Reason,
			&rec.Composite,
			&rec.Viability,
			&rec.ScoredAt,
		); scanErr != nil {
			continue
		}
		if rec.ScoredAt.After(newest) {
			newest = rec.ScoredAt
		}
		summary := fmt.Sprintf(
			"%s (%s) rejected in scoring: reason=%s composite=%.2f viability=%.2f",
			coalesce(strings.TrimSpace(rec.VerticalName), strings.TrimSpace(rec.VerticalID)),
			coalesce(strings.TrimSpace(rec.Geography), "unspecified"),
			coalesce(strings.TrimSpace(rec.Reason), "rejected"),
			rec.Composite,
			rec.Viability,
		)
		out = append(out, map[string]any{
			"id":              rec.ID,
			"vertical_id":     rec.VerticalID,
			"vertical_name":   rec.VerticalName,
			"geography":       rec.Geography,
			"result":          rec.Result,
			"reason":          rec.Reason,
			"summary":         summary,
			"composite_score": rec.Composite,
			"viability_score": rec.Viability,
			"occurred_at":     rec.ScoredAt.UTC().Format(time.RFC3339),
		})
	}
	if err := rows.Err(); err != nil {
		log.Printf("pipeline: consume scoring digest buffer iteration failed err=%v", err)
	}
	return out, newest
}

func (pc *FactoryPipelineCoordinator) isScoringDigestBufferEnabled(ctx context.Context) bool {
	if pc == nil || pc.db == nil {
		return false
	}
	pc.mu.Lock()
	if pc.scoringDigestBufferChecked {
		enabled := pc.scoringDigestBufferEnabled
		pc.mu.Unlock()
		return enabled
	}
	pc.mu.Unlock()

	var ok bool
	if err := dbQueryRowContext(ctx, pc.db, `SELECT to_regclass('public.scoring_digest_buffer') IS NOT NULL`).Scan(&ok); err != nil {
		ok = false
	}
	pc.mu.Lock()
	if !pc.scoringDigestBufferChecked {
		pc.scoringDigestBufferEnabled = ok
		pc.scoringDigestBufferChecked = true
	}
	enabled := pc.scoringDigestBufferEnabled
	pc.mu.Unlock()
	return enabled
}

func (pc *FactoryPipelineCoordinator) checkScoringTimeouts(ctx context.Context, now time.Time) {
	pc.mu.Lock()
	stale := make([]string, 0, len(pc.scoring))
	for verticalID, acc := range pc.scoring {
		if acc == nil {
			continue
		}
		if len(acc.Contested) > 0 {
			continue
		}
		ref := acc.RequestedAt
		if ref.IsZero() {
			ref = acc.LastUpdatedAt
		}
		if ref.IsZero() {
			ref = now
		}
		if now.Sub(ref) >= scoringTimeout {
			stale = append(stale, verticalID)
		}
	}
	pc.mu.Unlock()
	for _, verticalID := range stale {
		pc.finalizeScoringAccumulator(ctx, verticalID, true)
	}
}

func (pc *FactoryPipelineCoordinator) markShardCompletedByAgent(ctx context.Context, agentID string) string {
	if pc == nil || pc.db == nil || strings.TrimSpace(agentID) == "" {
		return ""
	}
	if !pc.isShardsTableEnabled(ctx) {
		return ""
	}
	var shardID string
	if err := dbQueryRowContext(ctx, pc.db, `
		UPDATE shards
		SET status = 'completed',
		    completed_at = COALESCE(completed_at, now())
		WHERE agent_id = $1
		  AND status = 'assigned'
		RETURNING id::text
	`, strings.TrimSpace(agentID)).Scan(&shardID); err != nil {
		return ""
	}
	return strings.TrimSpace(shardID)
}

func (pc *FactoryPipelineCoordinator) shardTerminalProgress(ctx context.Context, scanID string) (total, completed, failed int, ok bool) {
	if pc == nil || pc.db == nil || strings.TrimSpace(scanID) == "" {
		return 0, 0, 0, false
	}
	if !pc.isShardsTableEnabled(ctx) {
		return 0, 0, 0, false
	}
	scanUUID := stableUUID(scanID).String()
	if err := dbQueryRowContext(ctx, pc.db, `
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE status = 'completed') AS completed,
			COUNT(*) FILTER (WHERE status IN ('failed', 'timed_out')) AS failed
		FROM shards
		WHERE scan_id = $1::uuid
	`, scanUUID).Scan(&total, &completed, &failed); err != nil {
		return 0, 0, 0, false
	}
	return total, completed, failed, total > 0
}

func (pc *FactoryPipelineCoordinator) handleValidationStarted(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	scoringPayload := parsePayloadMap(evt.Payload)
	pc.mu.Lock()
	st := pc.validations[verticalID]
	if st == nil {
		st = &validationPipelineState{VerticalID: verticalID, Status: "active"}
		pc.validations[verticalID] = st
	}
	if st.Status == "" {
		st.Status = "active"
	}
	if st.Status == "parked" || st.Status == "rejected" {
		st.Status = "active"
	}
	if len(evt.Payload) > 0 {
		st.ScoringPayload = cloneRaw(evt.Payload)
	}
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	pc.mu.Unlock()

	pc.updateVerticalStage(ctx, verticalID, "researching", "")
	validationPayload := pc.buildValidationStartedPayload(ctx, verticalID, scoringPayload, nil)
	pc.publish(ctx, "validation.started", verticalID, payloadMap(validationPayload))
	brandPayload := pc.buildBrandRequestedPayload(ctx, verticalID, scoringPayload, nil)
	pc.publish(ctx, "brand.requested", verticalID, payloadMap(brandPayload))
}

func (pc *FactoryPipelineCoordinator) handleValidationGate(ctx context.Context, evt events.Event, gate string) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	payload := parsePayloadMap(evt.Payload)

	pc.mu.Lock()
	st := pc.getValidationStateLocked(verticalID)
	if st.Status == "rejected" || st.Status == "packaged" {
		pc.mu.Unlock()
		return
	}
	switch gate {
	case "g1":
		st.G1Research = true
		if len(st.ResearchPayload) > 0 && len(evt.Payload) > 0 {
			st.ResearchPayload = mergeRawPayload(st.ResearchPayload, evt.Payload)
		} else {
			st.ResearchPayload = cloneRaw(evt.Payload)
		}
	case "g2":
		st.G2Spec = true
		st.SpecPayload = cloneRaw(evt.Payload)
		st.InnerRevisionCount = 0
		st.SpecVersion++
	case "g3":
		st.G3CTO = true
		st.CTOPayload = cloneRaw(evt.Payload)
	case "g4":
		st.G4Brand = true
		st.BrandPayload = cloneRaw(evt.Payload)
	}
	st.Status = "active"
	shouldPackage := st.G1Research && st.G2Spec && st.G3CTO && st.G4Brand && !st.PackagingRequested
	stage := pc.validationStageForState(st)
	var bundle ValidationPackageReadyPayload
	hasBundle := false
	if shouldPackage {
		now := time.Now().UTC()
		st.PackagingRequestedAt = &now
		st.PackagingRetries = 0
		st.PackagingRequested = true
		hasBundle = true
		bundle = pc.buildValidationPackageReadyPayload(ctx, verticalID, validationContextSnapshot{
			Research:    parsePayloadMap(st.ResearchPayload),
			Spec:        parsePayloadMap(st.SpecPayload),
			CTONotes:    parsePayloadMap(st.CTOPayload),
			Brand:       parsePayloadMap(st.BrandPayload),
			Scoring:     parsePayloadMap(st.ScoringPayload),
			SpecVersion: st.SpecVersion,
		})
	}
	pc.mu.Unlock()

	pc.updateVerticalStage(ctx, verticalID, stage, "")
	if gate == "g2" {
		pc.publish(ctx, "spec.validation_requested", verticalID, payloadMap(pc.buildSpecValidationRequestedPayload(ctx, verticalID, payload)))
	}
	if hasBundle {
		pc.publish(ctx, "validation.package_ready", verticalID, payloadMap(bundle))
	}
}

func (pc *FactoryPipelineCoordinator) handleSpecValidationPassed(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if !pc.specVersionMatches(verticalID, payload) {
		return
	}
	pc.publish(ctx, "cto.spec_review_requested", verticalID, payloadMap(pc.buildCTOSpecReviewRequestedPayload(ctx, verticalID, payload)))
}

func (pc *FactoryPipelineCoordinator) handleSpecValidationFailed(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if !pc.specVersionMatches(verticalID, payload) {
		return
	}
	status := strings.ToLower(strings.TrimSpace(asString(payload["status"])))
	passed := strings.EqualFold(strings.TrimSpace(asString(payload["passed"])), "true")
	if status == "non-blocker" || passed {
		pc.publish(ctx, "cto.spec_review_requested", verticalID, payloadMap(pc.buildCTOSpecReviewRequestedPayload(ctx, verticalID, payload)))
		return
	}
	escalate := false
	pc.mu.Lock()
	st := pc.getValidationStateLocked(verticalID)
	st.G2Spec = false
	st.G3CTO = false
	st.SpecPayload = nil
	st.CTOPayload = nil
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	st.RevisionCount++
	if st.RevisionCount > maxRevisionCycles {
		st.Status = "parked"
		escalate = true
	}
	pc.mu.Unlock()
	pc.updateVerticalStage(ctx, verticalID, "mvp_speccing", "")
	if escalate {
		pc.parkVerticalWithMailbox(ctx, verticalID, "Vertical stuck in revision loop after repeated spec-auditor blockers.", payload)
		return
	}
	pc.publish(ctx, "spec.revision_requested", verticalID, payloadMap(pc.buildSpecRevisionRequestedPayload(ctx, verticalID, "spec-auditor", payload)))
}

func (pc *FactoryPipelineCoordinator) handleCTORevisionNeeded(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	escalate := false
	pc.mu.Lock()
	st := pc.getValidationStateLocked(verticalID)
	st.G2Spec = false
	st.G3CTO = false
	st.SpecPayload = nil
	st.CTOPayload = nil
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	st.RevisionCount++
	if st.RevisionCount > maxRevisionCycles {
		st.Status = "parked"
		escalate = true
	}
	pc.mu.Unlock()
	pc.updateVerticalStage(ctx, verticalID, "mvp_speccing", "")
	if escalate {
		pc.parkVerticalWithMailbox(ctx, verticalID, "Vertical stuck in revision loop after repeated CTO revisions.", parsePayloadMap(evt.Payload))
		return
	}
	pc.publish(ctx, "spec.revision_requested", verticalID, payloadMap(pc.buildSpecRevisionRequestedPayload(ctx, verticalID, "factory-cto", parsePayloadMap(evt.Payload))))
}

func (pc *FactoryPipelineCoordinator) handleValidationRejected(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	pc.mu.Lock()
	st := pc.getValidationStateLocked(verticalID)
	st.Status = "rejected"
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	pc.mu.Unlock()
	pc.updateVerticalStage(ctx, verticalID, "killed", string(evt.Type))
	pc.publish(ctx, "vertical.killed", verticalID, payloadMap(pc.buildVerticalKilledPayload(ctx, verticalID, string(evt.Type), parsePayloadMap(evt.Payload))))
}

func (pc *FactoryPipelineCoordinator) handleValidationPackaged(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	pc.mu.Lock()
	st := pc.getValidationStateLocked(verticalID)
	st.Status = "packaged"
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	pc.mu.Unlock()
	pc.updateVerticalStage(ctx, verticalID, "ready_for_review", "")
}

func (pc *FactoryPipelineCoordinator) handleValidationMoreData(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	pc.mu.Lock()
	st := pc.getValidationStateLocked(verticalID)
	st.Status = "active"
	st.G1Research = false
	st.ResearchPayload = nil
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	snap := validationContextSnapshot{
		Research: parsePayloadMap(st.ResearchPayload),
		Spec:     parsePayloadMap(st.SpecPayload),
		Scoring:  parsePayloadMap(st.ScoringPayload),
	}
	pc.mu.Unlock()
	pc.updateVerticalStage(ctx, verticalID, "researching", "")
	pc.publish(ctx, "validation.more_data_needed", verticalID, payloadMap(pc.buildValidationMoreDataPayload(ctx, verticalID, parsePayloadMap(evt.Payload), snap)))
}

func (pc *FactoryPipelineCoordinator) handleBrandRevision(ctx context.Context, evt events.Event) {
	if strings.TrimSpace(evt.SourceAgent) == "pipeline-coordinator" {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	feedback := parsePayloadMap(evt.Payload)
	pc.mu.Lock()
	st := pc.getValidationStateLocked(verticalID)
	brand := parsePayloadMap(st.BrandPayload)
	st.Status = "active"
	st.G4Brand = false
	st.BrandPayload = nil
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	pc.mu.Unlock()
	pc.updateVerticalStage(ctx, verticalID, "branding", "")
	pc.publish(ctx, "brand.revision_needed", verticalID, payloadMap(pc.buildBrandRevisionNeededPayload(ctx, verticalID, feedback, brand)))
}

func (pc *FactoryPipelineCoordinator) handleVerticalResumed(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	pc.mu.Lock()
	st := pc.getValidationStateLocked(verticalID)
	st.Status = "active"
	st.RevisionCount = 0
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	missingG1 := !st.G1Research
	missingG2 := !st.G2Spec
	missingG3 := !st.G3CTO
	missingG4 := !st.G4Brand
	all := st.G1Research && st.G2Spec && st.G3CTO && st.G4Brand
	stage := pc.validationStageForState(st)
	scoringRaw := cloneRaw(st.ScoringPayload)
	var bundle ValidationPackageReadyPayload
	hasBundle := false
	if all {
		now := time.Now().UTC()
		hasBundle = true
		bundle = pc.buildValidationPackageReadyPayload(ctx, verticalID, validationContextSnapshot{
			Research:    parsePayloadMap(st.ResearchPayload),
			Spec:        parsePayloadMap(st.SpecPayload),
			CTONotes:    parsePayloadMap(st.CTOPayload),
			Brand:       parsePayloadMap(st.BrandPayload),
			Scoring:     parsePayloadMap(st.ScoringPayload),
			SpecVersion: st.SpecVersion,
		})
		st.PackagingRequested = true
		st.PackagingRequestedAt = &now
	}
	pc.mu.Unlock()
	pc.updateVerticalStage(ctx, verticalID, stage, "")

	resumePayload := parsePayloadMap(evt.Payload)
	snap := pc.validationContext(verticalID)
	if missingG1 {
		scoringPayload := parsePayloadMap(scoringRaw)
		if len(scoringPayload) == 0 {
			scoringPayload = parsePayloadMap(evt.Payload)
		}
		pc.publish(ctx, "validation.started", verticalID, payloadMap(pc.buildValidationStartedPayload(ctx, verticalID, scoringPayload, resumePayload)))
	}
	if missingG2 {
		pc.publish(ctx, "spec.revision_requested", verticalID, payloadMap(pc.buildSpecRevisionRequestedPayload(ctx, verticalID, "resume", resumePayload)))
	}
	if missingG3 {
		pc.publish(ctx, "cto.spec_review_requested", verticalID, payloadMap(pc.buildCTOSpecReviewRequestedPayload(ctx, verticalID, resumePayload)))
	}
	if missingG4 {
		pc.publish(ctx, "brand.requested", verticalID, payloadMap(pc.buildBrandRequestedPayload(ctx, verticalID, snap.Scoring, snap.Research)))
	}
	if hasBundle {
		pc.publish(ctx, "validation.package_ready", verticalID, payloadMap(bundle))
	}
}

func (pc *FactoryPipelineCoordinator) handleCTOApproved(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if !pc.specVersionMatches(verticalID, payload) {
		return
	}
	pc.handleValidationGate(ctx, evt, "g3")
}

func (pc *FactoryPipelineCoordinator) handleVerticalApproved(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	pc.mu.Lock()
	st := pc.getValidationStateLocked(verticalID)
	st.Status = "approved"
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	pc.mu.Unlock()
	pc.updateVerticalStage(ctx, verticalID, "approved", "")
}

func (pc *FactoryPipelineCoordinator) handleVerticalKilled(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	pc.mu.Lock()
	st := pc.getValidationStateLocked(verticalID)
	st.Status = "rejected"
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	pc.mu.Unlock()
	pc.updateVerticalStage(ctx, verticalID, "killed", string(evt.Type))
}

func (pc *FactoryPipelineCoordinator) handleOpCoCEOReady(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		payload := parsePayloadMap(evt.Payload)
		verticalID = strings.TrimSpace(asString(payload["vertical_id"]))
	}
	if verticalID == "" {
		return
	}
	pc.updateVerticalStage(ctx, verticalID, "operating", "")
}

func (pc *FactoryPipelineCoordinator) handleInnerSpecRevision(ctx context.Context, evt events.Event) bool {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return false
	}
	escalate := false
	pc.mu.Lock()
	st := pc.getValidationStateLocked(verticalID)
	if st.Status != "active" {
		pc.mu.Unlock()
		return false
	}
	st.InnerRevisionCount++
	if st.InnerRevisionCount > maxInnerRevisions {
		st.Status = "parked"
		escalate = true
	}
	pc.mu.Unlock()
	if escalate {
		pc.parkVerticalWithMailbox(ctx, verticalID, "Spec creation stuck in revision loop after 5 cycles.", parsePayloadMap(evt.Payload))
	}
	return escalate
}

func (pc *FactoryPipelineCoordinator) handleSpecRevisionRequested(evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	st := pc.getValidationStateLocked(verticalID)
	st.InnerRevisionCount = 0
}

func (pc *FactoryPipelineCoordinator) specVersionMatches(verticalID string, payload map[string]any) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	st := pc.getValidationStateLocked(verticalID)
	if st.SpecVersion <= 0 {
		return true
	}
	got := asInt(payload["spec_version"])
	if got == 0 {
		return true
	}
	return got == st.SpecVersion
}

func (pc *FactoryPipelineCoordinator) parkVerticalWithMailbox(ctx context.Context, verticalID, summary string, details map[string]any) {
	if pc == nil || pc.db == nil {
		return
	}
	if details == nil {
		details = map[string]any{}
	}
	contextPayload := map[string]any{
		"vertical_id": verticalID,
		"source":      "pipeline-coordinator",
		"details":     details,
	}
	_, _ = dbExecContext(ctx, pc.db, `
		INSERT INTO mailbox (event_id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
		VALUES (NULL, NULLIF($1,'')::uuid, 'pipeline-coordinator', 'vertical_approval', 'high', 'pending', $2::jsonb, $3, now())
	`, strings.TrimSpace(verticalID), string(mustJSON(contextPayload)), strings.TrimSpace(summary))
	pc.updateVerticalStage(ctx, verticalID, "ready_for_review", "")
}

func (pc *FactoryPipelineCoordinator) checkPackagingTimeouts(ctx context.Context, now time.Time) {
	if pc == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	type timedOut struct {
		verticalID string
		retry      bool
		snapshot   validationContextSnapshot
	}
	expired := make([]timedOut, 0, 4)
	pc.mu.Lock()
	for _, st := range pc.validations {
		if st == nil || st.Status != "active" || st.PackagingRequestedAt == nil {
			continue
		}
		if now.Before(st.PackagingRequestedAt.Add(packagingTimeout)) {
			continue
		}
		if st.PackagingRetries == 0 {
			st.PackagingRetries = 1
			n := now
			st.PackagingRequestedAt = &n
			expired = append(expired, timedOut{
				verticalID: st.VerticalID,
				retry:      true,
				snapshot: validationContextSnapshot{
					Research:    parsePayloadMap(st.ResearchPayload),
					Spec:        parsePayloadMap(st.SpecPayload),
					CTONotes:    parsePayloadMap(st.CTOPayload),
					Brand:       parsePayloadMap(st.BrandPayload),
					Scoring:     parsePayloadMap(st.ScoringPayload),
					SpecVersion: st.SpecVersion,
				},
			})
			continue
		}
		st.Status = "parked"
		st.PackagingRequested = false
		st.PackagingRequestedAt = nil
		expired = append(expired, timedOut{verticalID: st.VerticalID, retry: false})
	}
	pc.mu.Unlock()

	for _, item := range expired {
		if item.retry {
			pc.publish(ctx, "validation.package_ready", item.verticalID, payloadMap(pc.buildValidationPackageReadyPayload(ctx, item.verticalID, item.snapshot)))
			continue
		}
		pc.parkVerticalWithMailbox(ctx, item.verticalID, "Validation packaging timed out after retry. Human intervention required.", map[string]any{"vertical_id": item.verticalID})
	}
}

func (pc *FactoryPipelineCoordinator) checkScanTimeouts(ctx context.Context, now time.Time) {
	if pc == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	type timedOutScan struct {
		scanID       string
		campaignID   string
		mode         string
		geography    string
		reports      int
		expected     int
		completed    int
		discovered   int
		skipped      int
		pendingDedup int
		shardScanID  string
	}
	expired := make([]timedOutScan, 0, 8)
	pc.mu.Lock()
	for scanID, acc := range pc.scans {
		if acc == nil {
			continue
		}
		createdAt := acc.CreatedAt
		if createdAt.IsZero() {
			createdAt = now
		}
		if now.Before(createdAt.Add(scanTimeout)) {
			continue
		}
		expired = append(expired, timedOutScan{
			scanID:       acc.ScanID,
			campaignID:   acc.CampaignID,
			mode:         acc.Mode,
			geography:    acc.Geography,
			reports:      acc.Reports,
			expected:     maxInt(acc.Expected, 1),
			completed:    len(acc.CompletedBy),
			discovered:   acc.Discovered,
			skipped:      acc.Skipped,
			pendingDedup: pc.pendingDedupCountForScan(scanID),
			shardScanID:  scanID,
		})
		delete(pc.scans, scanID)
	}
	pc.mu.Unlock()

	for _, scan := range expired {
		stats := pc.buildScanCompletedPayload(scanCompletedBuildInput{
			ScanID:          scan.scanID,
			CampaignID:      scan.campaignID,
			Mode:            scan.mode,
			Geography:       scan.geography,
			ReportsReceived: scan.reports,
			Expected:        scan.expected,
			Complete:        scan.completed,
			Discovered:      scan.discovered,
			Skipped:         scan.skipped,
			PendingDedup:    scan.pendingDedup,
			TimedOut:        true,
		})
		shardTotal, shardCompleted, shardFailed, hasShardProgress := pc.shardTerminalProgress(ctx, scan.shardScanID)
		if hasShardProgress {
			terminal := shardCompleted + shardFailed
			stats.Expected = shardTotal
			stats.Complete = terminal
			stats.ShardsTotal = shardTotal
			stats.ShardsCompleted = shardCompleted
			stats.ShardsFailed = shardFailed
		}
		pc.publish(ctx, "scan.completed", "", payloadMap(stats))
	}
}

func (pc *FactoryPipelineCoordinator) publish(ctx context.Context, eventType, verticalID string, payload map[string]any) {
	if pc == nil {
		return
	}
	if payload == nil {
		payload = map[string]any{}
	}
	sourceAgent := pipelineSourceAgent(ctx)
	if sourceAgent == "" {
		sourceAgent = "pipeline-coordinator"
	}
	emitted := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: sourceAgent,
		VerticalID:  strings.TrimSpace(verticalID),
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		*collector = append(*collector, emitted)
		return
	}
	if pc.bus == nil {
		runtimeWarn(
			"pipeline-coordinator",
			"dropping emit because event bus is nil event_type=%s vertical_id=%s",
			strings.TrimSpace(eventType),
			strings.TrimSpace(verticalID),
		)
		return
	}
	if err := pc.bus.Publish(ctx, emitted); err != nil {
		runtimeWarn(
			"pipeline-coordinator",
			"failed to publish runtime event event_type=%s event_id=%s vertical_id=%s: %v",
			strings.TrimSpace(eventType),
			strings.TrimSpace(emitted.ID),
			strings.TrimSpace(verticalID),
			err,
		)
	}
}

func (pc *FactoryPipelineCoordinator) publishDirect(ctx context.Context, eventType, verticalID string, payload map[string]any, recipients []string) {
	if pc == nil {
		return
	}
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		pc.publish(ctx, eventType, verticalID, payload)
		return
	}
	if payload == nil {
		payload = map[string]any{}
	}
	sourceAgent := pipelineSourceAgent(ctx)
	if sourceAgent == "" {
		sourceAgent = "pipeline-coordinator"
	}
	emitted := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: sourceAgent,
		VerticalID:  strings.TrimSpace(verticalID),
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		*collector = append(*collector, emitted)
		return
	}
	if pc.bus == nil {
		runtimeWarn(
			"pipeline-coordinator",
			"dropping direct emit because event bus is nil event_type=%s vertical_id=%s recipients=%v",
			strings.TrimSpace(eventType),
			strings.TrimSpace(verticalID),
			recipients,
		)
		return
	}
	if err := pc.bus.PublishDirect(ctx, emitted, recipients); err != nil {
		runtimeWarn(
			"pipeline-coordinator",
			"failed to publish direct runtime event event_type=%s event_id=%s vertical_id=%s recipients=%v: %v",
			strings.TrimSpace(eventType),
			strings.TrimSpace(emitted.ID),
			strings.TrimSpace(verticalID),
			recipients,
			err,
		)
	}
}

func (pc *FactoryPipelineCoordinator) getValidationStateLocked(verticalID string) *validationPipelineState {
	st := pc.validations[verticalID]
	if st == nil {
		st = &validationPipelineState{VerticalID: verticalID, Status: "active"}
		pc.validations[verticalID] = st
	}
	if st.Status == "" {
		st.Status = "active"
	}
	return st
}

func (pc *FactoryPipelineCoordinator) validationStageForState(st *validationPipelineState) string {
	if st == nil {
		return "researching"
	}
	switch strings.TrimSpace(st.Status) {
	case "packaged":
		return "ready_for_review"
	case "parked":
		return "ready_for_review"
	case "approved":
		return "approved"
	case "rejected":
		return "killed"
	}
	if !st.G1Research {
		return "researching"
	}
	if !st.G2Spec {
		if st.InnerRevisionCount > 0 {
			return "spec_review"
		}
		return "mvp_speccing"
	}
	if !st.G3CTO {
		return "cto_spec_review"
	}
	if !st.G4Brand {
		return "branding"
	}
	return "branding"
}

func (pc *FactoryPipelineCoordinator) updateVerticalStage(ctx context.Context, verticalID, stage, sourceEvent string) {
	if pc == nil || pc.db == nil {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	stage = strings.TrimSpace(stage)
	sourceEvent = strings.TrimSpace(sourceEvent)
	if verticalID == "" || stage == "" {
		return
	}
	if stage == "ready_for_review" {
		_, _ = dbExecContext(ctx, pc.db, `
			UPDATE verticals
			SET stage = $2,
			    parked_at = COALESCE(parked_at, now()),
			    updated_at = now()
			WHERE id = $1::uuid
		`, verticalID, stage)
		return
	}
	if stage == "approved" {
		_, _ = dbExecContext(ctx, pc.db, `
			UPDATE verticals
			SET stage = $2,
			    approved_at = COALESCE(approved_at, now()),
			    parked_at = NULL,
			    updated_at = now()
			WHERE id = $1::uuid
		`, verticalID, stage)
		return
	}
	if stage == "killed" {
		_, _ = dbExecContext(ctx, pc.db, `
			UPDATE verticals
			SET stage = $2,
			    kill_reason = CASE
					WHEN COALESCE(kill_reason,'') = '' THEN NULLIF($3,'')
					ELSE kill_reason
				END,
			    killed_at_stage = CASE
					WHEN COALESCE(killed_at_stage,'') = '' THEN NULLIF($3,'')
					ELSE killed_at_stage
				END,
			    updated_at = now()
			WHERE id = $1::uuid
		`, verticalID, stage, sourceEvent)
		return
	}
	_, _ = dbExecContext(ctx, pc.db, `
		UPDATE verticals
		SET stage = $2,
		    updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, stage)
}

func (pc *FactoryPipelineCoordinator) pendingDedupCountForScan(scanID string) int {
	count := 0
	for _, cand := range pc.pendingDedup {
		if cand.ScanID == scanID {
			count++
		}
	}
	return count
}

type verticalCandidate struct {
	ID   string
	Name string
}

func (pc *FactoryPipelineCoordinator) loadVerticalsByGeography(ctx context.Context, geography string) ([]verticalCandidate, error) {
	if pc == nil || pc.db == nil || strings.TrimSpace(geography) == "" {
		return nil, nil
	}
	rows, err := dbQueryContext(ctx, pc.db, `
		SELECT id::text, name
		FROM verticals
		WHERE lower(geography) = lower($1)
		ORDER BY created_at DESC
		LIMIT 500
	`, geography)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]verticalCandidate, 0, 32)
	for rows.Next() {
		var v verticalCandidate
		if err := rows.Scan(&v.ID, &v.Name); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (pc *FactoryPipelineCoordinator) loadVerticalIdentity(ctx context.Context, verticalID string) (string, string, error) {
	if pc == nil || pc.db == nil || strings.TrimSpace(verticalID) == "" {
		return "", "", nil
	}
	var name, geography string
	err := dbQueryRowContext(ctx, pc.db, `
		SELECT COALESCE(name, ''), COALESCE(geography, '')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&name, &geography)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	return strings.TrimSpace(name), strings.TrimSpace(geography), nil
}

func (pc *FactoryPipelineCoordinator) validationContext(verticalID string) validationContextSnapshot {
	if pc == nil || strings.TrimSpace(verticalID) == "" {
		return validationContextSnapshot{
			Research: map[string]any{},
			Spec:     map[string]any{},
			CTONotes: map[string]any{},
			Brand:    map[string]any{},
			Scoring:  map[string]any{},
		}
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	st := pc.getValidationStateLocked(verticalID)
	return validationContextSnapshot{
		Research:    parsePayloadMap(st.ResearchPayload),
		Spec:        parsePayloadMap(st.SpecPayload),
		CTONotes:    parsePayloadMap(st.CTOPayload),
		Brand:       parsePayloadMap(st.BrandPayload),
		Scoring:     parsePayloadMap(st.ScoringPayload),
		SpecVersion: st.SpecVersion,
	}
}

func (pc *FactoryPipelineCoordinator) identityForPayload(ctx context.Context, verticalID string) (string, string) {
	name, geography, err := pc.loadVerticalIdentity(ctx, verticalID)
	if err != nil {
		log.Printf("pipeline: identity lookup failed vertical=%s err=%v", verticalID, err)
		return "", ""
	}
	return strings.TrimSpace(name), strings.TrimSpace(geography)
}

func (pc *FactoryPipelineCoordinator) buildScanAssignedPayload(
	scanID, campaignID, mode, geography string,
	source map[string]any,
	plannedShards int,
) ScanAssignedPayload {
	if source == nil {
		source = map[string]any{}
	}
	return ScanAssignedPayload{
		ScanID:             strings.TrimSpace(scanID),
		CampaignID:         strings.TrimSpace(campaignID),
		Mode:               strings.TrimSpace(mode),
		Geography:          strings.TrimSpace(geography),
		GeographyID:        strings.TrimSpace(asString(source["geography_id"])),
		TaxonomyCategories: source["taxonomy_categories"],
		Priority:           strings.TrimSpace(asString(source["priority"])),
		CampaignContext:    source["campaign_context"],
		DirectiveID:        strings.TrimSpace(asString(source["directive_id"])),
		StrategicContext:   source["strategic_context"],
		CorpusPath:         strings.TrimSpace(asString(source["corpus_path"])),
		CorpusSignals:      source["corpus_signals"],
		RequestedAt:        time.Now().UTC().Format(time.RFC3339),
		PlannedShards:      plannedShards,
	}
}

func (pc *FactoryPipelineCoordinator) buildSynthesisNeededPayload(scanID string, acc *scanAccumulator, raw map[string]any) SynthesisNeededPayload {
	if raw == nil {
		raw = map[string]any{}
	}
	out := SynthesisNeededPayload{
		ScanID:        strings.TrimSpace(scanID),
		Geography:     firstNonEmptyString(strings.TrimSpace(asString(raw["geography"])), strings.TrimSpace(asString(raw["geography_label"]))),
		Category:      strings.TrimSpace(asString(raw["category"])),
		Subcategory:   strings.TrimSpace(asString(raw["subcategory"])),
		ConflictNotes: raw["conflict_notes"],
		RawReport:     raw,
	}
	if acc != nil {
		out.CampaignID = strings.TrimSpace(acc.CampaignID)
		out.Mode = strings.TrimSpace(acc.Mode)
		out.Geography = firstNonEmptyString(strings.TrimSpace(out.Geography), strings.TrimSpace(acc.Geography))
	}
	return out
}

func (pc *FactoryPipelineCoordinator) buildDedupAmbiguousPayload(
	scanID, dedupEventID string,
	similarity float64,
	candidateName, geography string,
	signal float64,
	existingID, existingName string,
) DedupAmbiguousPayload {
	return DedupAmbiguousPayload{
		ScanID:       strings.TrimSpace(scanID),
		DedupEventID: strings.TrimSpace(dedupEventID),
		Similarity:   similarity,
		NewCandidate: DedupCandidatePayload{
			Name:           strings.TrimSpace(candidateName),
			Geography:      strings.TrimSpace(geography),
			SignalStrength: signal,
		},
		ExistingVertical: DedupCandidatePayload{
			ID:        strings.TrimSpace(existingID),
			Name:      strings.TrimSpace(existingName),
			Geography: strings.TrimSpace(geography),
		},
	}
}

func (pc *FactoryPipelineCoordinator) buildVerticalDiscoveredPayload(
	verticalID, name, geography, mode, scanID, campaignID string,
	signal float64,
	discoverySource string,
	rawSignals map[string]any,
) VerticalDiscoveredPayload {
	if rawSignals == nil {
		rawSignals = map[string]any{}
	}
	discoveryContext := buildDiscoveryContextPayload(rawSignals)
	geographicScope := normalizeGeographicScope(asString(rawSignals["geographic_scope"]))
	return VerticalDiscoveredPayload{
		VerticalID:           strings.TrimSpace(verticalID),
		VerticalName:         strings.TrimSpace(name),
		Name:                 strings.TrimSpace(name),
		Geography:            strings.TrimSpace(geography),
		GeographicScope:      geographicScope,
		Mode:                 strings.TrimSpace(mode),
		ScanID:               strings.TrimSpace(scanID),
		CampaignID:           strings.TrimSpace(campaignID),
		SignalStrength:       signal,
		OpportunityPattern:   normalizeOpportunityPattern(asString(rawSignals["opportunity_pattern"])),
		SignalSources:        rawSignals["signal_sources"],
		RequiredCapabilities: rawSignals["required_capabilities"],
		DiscoverySource:      strings.TrimSpace(discoverySource),
		RawSignals:           rawSignals,
		DiscoveryContext:     discoveryContext,
	}
}

type scanCompletedBuildInput struct {
	ScanID          string
	CampaignID      string
	Mode            string
	Geography       string
	ReportsReceived int
	Expected        int
	Complete        int
	Discovered      int
	Skipped         int
	PendingDedup    int
	TimedOut        bool
}

func (pc *FactoryPipelineCoordinator) buildScanCompletedPayload(in scanCompletedBuildInput) ScanCompletedPayload {
	return ScanCompletedPayload{
		ScanID:          strings.TrimSpace(in.ScanID),
		CampaignID:      strings.TrimSpace(in.CampaignID),
		Mode:            strings.TrimSpace(in.Mode),
		Geography:       strings.TrimSpace(in.Geography),
		ReportsReceived: in.ReportsReceived,
		Expected:        in.Expected,
		Complete:        in.Complete,
		Discovered:      in.Discovered,
		Skipped:         in.Skipped,
		PendingDedup:    in.PendingDedup,
		TimedOut:        in.TimedOut,
	}
}

func (pc *FactoryPipelineCoordinator) buildScoringRequestedPayload(verticalID string, acc *scoringAccumulator) ScoringRequestedPayload {
	if acc == nil {
		return ScoringRequestedPayload{
			VerticalID:          strings.TrimSpace(verticalID),
			DimensionsRequested: []string{},
			DiscoveryContext:    map[string]any{},
		}
	}
	dimensions := []string{}
	if len(acc.Expected) > 0 {
		dimensions = append([]string{}, acc.Expected...)
	}
	discoveryContext := map[string]any{}
	if len(acc.DiscoveryContext) > 0 {
		discoveryContext = cloneMap(acc.DiscoveryContext)
	}
	return ScoringRequestedPayload{
		VerticalID:          strings.TrimSpace(verticalID),
		VerticalName:        strings.TrimSpace(acc.VerticalName),
		Geography:           strings.TrimSpace(acc.Geography),
		Mode:                strings.TrimSpace(acc.Mode),
		Rubric:              strings.TrimSpace(acc.Rubric),
		DimensionsRequested: dimensions,
		DiscoveryContext:    discoveryContext,
	}
}

func (pc *FactoryPipelineCoordinator) derivedScoringGeneratorAgent(ctx context.Context, acc *scoringAccumulator) string {
	if acc == nil {
		return ""
	}
	derivedContext := normalizeScanMode(acc.Mode) == "derived"
	if !derivedContext {
		if strings.TrimSpace(asString(acc.DiscoveryContext["parent_id"])) != "" {
			derivedContext = true
		}
		if intFromAny(acc.DiscoveryContext["generation_depth"]) > 0 {
			derivedContext = true
		}
	}
	if !derivedContext {
		return ""
	}

	raw := strings.TrimSpace(asString(acc.DiscoveryContext["generator_agent_id"]))
	if raw == "" {
		return ""
	}
	if strings.Contains(strings.ToLower(raw), "analysis-agent") {
		return raw
	}
	if pc == nil || pc.db == nil {
		return raw
	}

	// In derived flows the generator can arrive as session ID; resolve to agent_id when possible.
	var agentID string
	_ = dbQueryRowContext(ctx, pc.db, `
		SELECT COALESCE(agent_id, '')
		FROM agent_sessions
		WHERE id = $1
		ORDER BY started_at DESC
		LIMIT 1
	`, raw).Scan(&agentID)
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return raw
	}
	return agentID
}

func (pc *FactoryPipelineCoordinator) selectScoringAnalysisRecipient(excludedAgent string) string {
	if pc == nil || pc.bus == nil {
		return ""
	}
	excludedAgent = strings.TrimSpace(excludedAgent)
	recipients := uniqueStrings(pc.bus.resolveSubscribedRecipients("scoring.requested"))
	if len(recipients) == 0 {
		return ""
	}
	sort.Strings(recipients)
	for _, recipient := range recipients {
		candidate := strings.TrimSpace(recipient)
		if candidate == "" || !strings.Contains(strings.ToLower(candidate), "analysis-agent") {
			continue
		}
		if excludedAgent != "" && strings.EqualFold(candidate, excludedAgent) {
			continue
		}
		return candidate
	}
	return ""
}

func buildDiscoveryContextPayload(raw map[string]any) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	out := map[string]any{}
	if v := strings.TrimSpace(asString(raw["opportunity_name"])); v != "" {
		out["opportunity_name"] = v
	}
	if v := strings.TrimSpace(asString(raw["preliminary_icp"])); v != "" {
		out["preliminary_icp"] = v
	}
	if buildSketch, ok := asObject(raw["build_sketch"]); ok && len(buildSketch) > 0 {
		out["build_sketch"] = cloneMap(buildSketch)
		if redFlags, ok := asArray(buildSketch["red_flags"]); ok && len(redFlags) > 0 {
			out["red_flags_passthrough"] = redFlags
		}
	}
	if evidence, ok := asObject(raw["evidence"]); ok && len(evidence) > 0 {
		out["evidence"] = cloneMap(evidence)
	}
	if v := strings.TrimSpace(asString(raw["opportunity_hypothesis"])); v != "" {
		out["opportunity_hypothesis"] = v
	}
	if v := normalizeOpportunityPattern(asString(raw["opportunity_pattern"])); v != "" {
		out["opportunity_pattern"] = v
	}
	if sources := raw["signal_sources"]; sources != nil {
		out["signal_sources"] = sources
	}
	if caps := raw["required_capabilities"]; caps != nil {
		out["required_capabilities"] = caps
	}
	if parentID := strings.TrimSpace(asString(raw["parent_id"])); parentID != "" {
		out["parent_id"] = parentID
	}
	if depth := intFromAny(raw["generation_depth"]); depth > 0 {
		out["generation_depth"] = depth
	}
	if generator := strings.TrimSpace(asString(raw["generator_agent_id"])); generator != "" {
		out["generator_agent_id"] = generator
	}
	if rationale, ok := asObject(raw["derivation_rationale"]); ok && len(rationale) > 0 {
		out["derivation_rationale"] = cloneMap(rationale)
	}
	return out
}

func normalizeOpportunityPattern(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		return ""
	}
	allowed := map[string]struct{}{
		"platform_parasitic":     {},
		"freelancer_replacement": {},
		"data_asymmetry":         {},
		"api_middleware":         {},
		"compliance_regulatory":  {},
		"ai_wrapper":             {},
		"workflow_automation":    {},
		"unknown":                {},
	}
	if _, ok := allowed[v]; ok {
		return v
	}
	return "unknown"
}

func normalizeGeographicScope(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "global":
		return "global"
	case "regional":
		return "regional"
	case "local":
		return "local"
	default:
		return "local"
	}
}

func (pc *FactoryPipelineCoordinator) buildScoringContestedPayload(verticalID, dimension string, contest contestedDimension, acc *scoringAccumulator) ScoringContestedPayload {
	out := ScoringContestedPayload{
		VerticalID: strings.TrimSpace(verticalID),
		Dimension:  strings.TrimSpace(dimension),
		Scores:     append([]int{}, contest.Scores...),
		Evidence:   append([]string{}, contest.Evidence...),
		Spread:     contest.Spread,
	}
	if acc != nil {
		out.Rubric = strings.TrimSpace(acc.Rubric)
		out.Mode = strings.TrimSpace(acc.Mode)
	}
	return out
}

func (pc *FactoryPipelineCoordinator) buildVerticalScoredPayload(verticalID string, result scoringComposite, acc *scoringAccumulator) VerticalScoredPayload {
	out := VerticalScoredPayload{
		VerticalID:     strings.TrimSpace(verticalID),
		Result:         strings.TrimSpace(result.Result),
		Reason:         strings.TrimSpace(result.Reason),
		CompositeScore: result.CompositeScore,
		ViabilityScore: result.ViabilityScore,
		MarketScore:    result.MarketScore,
		Dimensions:     result.Dimensions,
		Rubric:         strings.TrimSpace(result.Rubric),
		Partial:        result.Partial,
	}
	if out.Dimensions == nil {
		out.Dimensions = map[string]scoreDimensionResult{}
	}
	if acc != nil {
		out.Mode = strings.TrimSpace(acc.Mode)
		out.VerticalName = strings.TrimSpace(acc.VerticalName)
		out.Geography = strings.TrimSpace(acc.Geography)
	}
	return out
}

func (pc *FactoryPipelineCoordinator) buildVerticalShortlistedPayload(verticalID string, composite, viability float64, scoringPayload map[string]any) VerticalShortlistedPayload {
	if scoringPayload == nil {
		scoringPayload = map[string]any{}
	}
	return VerticalShortlistedPayload{
		VerticalID:     strings.TrimSpace(verticalID),
		CompositeScore: composite,
		ViabilityScore: viability,
		ScoringPayload: scoringPayload,
	}
}

func (pc *FactoryPipelineCoordinator) buildVerticalMarginalPayload(verticalID string, result scoringComposite) VerticalMarginalPayload {
	dim := result.Dimensions
	if dim == nil {
		dim = map[string]scoreDimensionResult{}
	}
	return VerticalMarginalPayload{
		VerticalID:        strings.TrimSpace(verticalID),
		CompositeScore:    result.CompositeScore,
		ViabilityScore:    result.ViabilityScore,
		Dimensions:        dim,
		PromotionEligible: true,
	}
}

func (pc *FactoryPipelineCoordinator) buildVerticalRejectedPayload(verticalID string, result scoringComposite) VerticalRejectedPayload {
	return VerticalRejectedPayload{
		VerticalID: strings.TrimSpace(verticalID),
		Reason:     strings.TrimSpace(result.Reason),
	}
}

func (pc *FactoryPipelineCoordinator) buildBrandRequestedPayload(ctx context.Context, verticalID string, scoring map[string]any, brief map[string]any) BrandRequestedPayload {
	if scoring == nil {
		scoring = map[string]any{}
	}
	if brief == nil {
		brief = map[string]any{}
	}
	name, geography := pc.identityForPayload(ctx, verticalID)
	return BrandRequestedPayload{
		VerticalID:    strings.TrimSpace(verticalID),
		VerticalName:  name,
		Name:          name,
		Geography:     geography,
		Scoring:       scoring,
		BusinessBrief: brief,
	}
}

func (pc *FactoryPipelineCoordinator) buildValidationPackageReadyPayload(ctx context.Context, verticalID string, snap validationContextSnapshot) ValidationPackageReadyPayload {
	name, geography := pc.identityForPayload(ctx, verticalID)
	if snap.Research == nil {
		snap.Research = map[string]any{}
	}
	if snap.Spec == nil {
		snap.Spec = map[string]any{}
	}
	if snap.CTONotes == nil {
		snap.CTONotes = map[string]any{}
	}
	if snap.Brand == nil {
		snap.Brand = map[string]any{}
	}
	if snap.Scoring == nil {
		snap.Scoring = map[string]any{}
	}
	return ValidationPackageReadyPayload{
		VerticalID:   strings.TrimSpace(verticalID),
		VerticalName: name,
		Geography:    geography,
		Research:     snap.Research,
		Spec:         snap.Spec,
		CTONotes:     snap.CTONotes,
		Brand:        snap.Brand,
		Scoring:      snap.Scoring,
		SpecVersion:  snap.SpecVersion,
	}
}

func (pc *FactoryPipelineCoordinator) buildSpecValidationRequestedPayload(ctx context.Context, verticalID string, spec map[string]any) SpecValidationRequestedPayload {
	if spec == nil {
		spec = map[string]any{}
	}
	specTier := strings.TrimSpace(asString(spec["spec_tier"]))
	if specTier == "" {
		specTier = strings.TrimSpace(asString(spec["spec_type"]))
	}
	if specTier == "" {
		specTier = "vertical_spec"
	}
	return SpecValidationRequestedPayload{
		VerticalID:  strings.TrimSpace(verticalID),
		SpecContent: spec,
		SpecTier:    specTier,
	}
}

func (pc *FactoryPipelineCoordinator) buildCTOSpecReviewRequestedPayload(ctx context.Context, verticalID string, specValidation map[string]any) CTOSpecReviewRequestedPayload {
	if specValidation == nil {
		specValidation = map[string]any{}
	}
	snap := pc.validationContext(verticalID)
	name, geography := pc.identityForPayload(ctx, verticalID)
	specVersion := asInt(specValidation["spec_version"])
	if specVersion == 0 {
		specVersion = snap.SpecVersion
	}
	return CTOSpecReviewRequestedPayload{
		VerticalID:     strings.TrimSpace(verticalID),
		VerticalName:   name,
		Geography:      geography,
		SpecValidation: specValidation,
		SpecVersion:    specVersion,
		Research:       snap.Research,
		Spec:           snap.Spec,
		Scoring:        snap.Scoring,
	}
}

func (pc *FactoryPipelineCoordinator) buildSpecRevisionRequestedPayload(ctx context.Context, verticalID, source string, feedback map[string]any) SpecRevisionRequestedPayload {
	if feedback == nil {
		feedback = map[string]any{}
	}
	snap := pc.validationContext(verticalID)
	name, geography := pc.identityForPayload(ctx, verticalID)
	return SpecRevisionRequestedPayload{
		VerticalID:   strings.TrimSpace(verticalID),
		VerticalName: name,
		Geography:    geography,
		Source:       strings.TrimSpace(source),
		Feedback:     feedback,
		Research:     snap.Research,
		Spec:         snap.Spec,
		Scoring:      snap.Scoring,
	}
}

func (pc *FactoryPipelineCoordinator) buildValidationMoreDataPayload(ctx context.Context, verticalID string, request map[string]any, snap validationContextSnapshot) ValidationMoreDataNeededPayload {
	if request == nil {
		request = map[string]any{}
	}
	name, geography := pc.identityForPayload(ctx, verticalID)
	return ValidationMoreDataNeededPayload{
		VerticalID:   strings.TrimSpace(verticalID),
		VerticalName: name,
		Geography:    geography,
		Request:      request,
		Research:     snap.Research,
		Spec:         snap.Spec,
		Scoring:      snap.Scoring,
	}
}

func (pc *FactoryPipelineCoordinator) buildBrandRevisionNeededPayload(ctx context.Context, verticalID string, feedback map[string]any, brand map[string]any) BrandRevisionNeededPayload {
	if feedback == nil {
		feedback = map[string]any{}
	}
	if brand == nil {
		brand = map[string]any{}
	}
	name, geography := pc.identityForPayload(ctx, verticalID)
	return BrandRevisionNeededPayload{
		VerticalID:   strings.TrimSpace(verticalID),
		VerticalName: name,
		Geography:    geography,
		Feedback:     feedback,
		Brand:        brand,
	}
}

func (pc *FactoryPipelineCoordinator) buildVerticalKilledPayload(ctx context.Context, verticalID, sourceEvent string, reason map[string]any) VerticalKilledPayload {
	if reason == nil {
		reason = map[string]any{}
	}
	name, geography := pc.identityForPayload(ctx, verticalID)
	return VerticalKilledPayload{
		VerticalID:   strings.TrimSpace(verticalID),
		VerticalName: name,
		Geography:    geography,
		SourceEvent:  strings.TrimSpace(sourceEvent),
		Priority:     "high",
		Reason:       reason,
	}
}

func (pc *FactoryPipelineCoordinator) buildValidationStartedPayload(ctx context.Context, verticalID string, scoring map[string]any, seed map[string]any) ValidationStartedPayload {
	if scoring == nil {
		scoring = map[string]any{}
	}
	if seed == nil {
		seed = map[string]any{}
	}
	out := ValidationStartedPayload{
		VerticalID: strings.TrimSpace(verticalID),
		Scoring:    scoring,
	}

	name := firstNonEmptyString(
		asString(seed["vertical_name"]),
		asString(seed["name"]),
		asString(scoring["vertical_name"]),
		asString(scoring["name"]),
	)
	geography := firstNonEmptyString(
		asString(seed["geography"]),
		asString(scoring["geography"]),
	)
	dbName, dbGeography, err := pc.loadVerticalIdentity(ctx, verticalID)
	if err != nil {
		log.Printf("pipeline: validation payload enrichment failed vertical=%s err=%v", verticalID, err)
	} else {
		if strings.TrimSpace(dbName) != "" {
			name = dbName
		}
		if strings.TrimSpace(dbGeography) != "" {
			geography = dbGeography
		}
	}
	if strings.TrimSpace(name) != "" {
		out.VerticalName = strings.TrimSpace(name)
		out.Name = strings.TrimSpace(name)
	}
	if strings.TrimSpace(geography) != "" {
		out.Geography = strings.TrimSpace(geography)
	}
	return out
}

func (pc *FactoryPipelineCoordinator) ensureVerticalDiscovered(ctx context.Context, name, geography, mode string, payload map[string]any) (string, error) {
	existing, err := pc.loadVerticalsByGeography(ctx, geography)
	if err != nil {
		return "", err
	}
	norm := normalizeName(name)
	for _, v := range existing {
		if normalizeName(v.Name) == norm {
			return v.ID, nil
		}
	}
	verticalID := uuid.NewString()
	if pc == nil || pc.db == nil {
		return verticalID, nil
	}
	slug := buildVerticalSlug(name, verticalID)
	if _, err := dbExecContext(ctx, pc.db, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, raw_signals, created_at, updated_at)
		VALUES ($1::uuid, $2, $3, $4, 'discovered', 'factory', $5::jsonb, now(), now())
	`, verticalID, name, slug, geography, string(mustJSON(payload))); err != nil {
		return "", err
	}
	_ = pc.updateVerticalDiscoveryMetadata(ctx, verticalID, mode, payload)
	return verticalID, nil
}

func (pc *FactoryPipelineCoordinator) updateVerticalDiscoveryMetadata(ctx context.Context, verticalID, mode string, payload map[string]any) error {
	if pc == nil || pc.db == nil || strings.TrimSpace(verticalID) == "" {
		return nil
	}
	if payload == nil {
		payload = map[string]any{}
	}
	discoveryMode := normalizeScanMode(mode)
	if discoveryMode == "" {
		discoveryMode = strings.ToLower(strings.TrimSpace(mode))
	}
	if discoveryMode == "" {
		discoveryMode = strings.ToLower(strings.TrimSpace(asString(payload["mode"])))
	}
	if discoveryMode == "" {
		discoveryMode = "saas_gap"
	}
	opportunityPattern := normalizeOpportunityPattern(asString(payload["opportunity_pattern"]))
	if opportunityPattern == "" {
		opportunityPattern = "unknown"
	}
	signalSources := payload["signal_sources"]
	if signalSources == nil {
		signalSources = []any{}
	}
	requiredCapabilities := payload["required_capabilities"]
	if requiredCapabilities == nil {
		requiredCapabilities = map[string]any{}
	}
	parentID := strings.TrimSpace(asString(payload["parent_id"]))
	generationDepth := intFromAny(payload["generation_depth"])
	if generationDepth < 0 {
		generationDepth = 0
	}
	if generationDepth > 2 {
		generationDepth = 2
	}
	generatorAgentID := strings.TrimSpace(asString(payload["generator_agent_id"]))
	derivationRationale := payload["derivation_rationale"]
	if derivationRationale == nil {
		derivationRationale = map[string]any{}
	}
	_, err := dbExecContext(ctx, pc.db, `
		UPDATE verticals
		SET
			discovery_mode = $2,
			opportunity_pattern = COALESCE(NULLIF($3, ''), opportunity_pattern),
			signal_sources = COALESCE($4::jsonb, signal_sources),
			required_capabilities = COALESCE($5::jsonb, required_capabilities),
			parent_id = COALESCE(NULLIF($6, '')::uuid, parent_id),
			generation_depth = CASE WHEN $7 > 0 THEN $7 ELSE generation_depth END,
			generator_agent_id = COALESCE(NULLIF($8, ''), generator_agent_id),
			derivation_rationale = COALESCE($9::jsonb, derivation_rationale),
			updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, discoveryMode, opportunityPattern, string(mustJSON(signalSources)), string(mustJSON(requiredCapabilities)), parentID, generationDepth, generatorAgentID, string(mustJSON(derivationRationale)))
	if err != nil {
		// Older test fixtures may not include newer columns; ignore metadata enrichment failures.
		return err
	}
	return nil
}

func expectedAgents(mode string) int {
	switch normalizeScanMode(mode) {
	case "automation_micro", "saas_gap", "saas_trend", "corpus":
		return 1
	case "local_services":
		return localServicesScannerExpected
	default:
		return 1
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

var nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)
var punctuationHeavyName = regexp.MustCompile(`[.!?;:]`)

var knownVerticalAcronyms = map[string]string{
	"ai":    "AI",
	"api":   "API",
	"b2b":   "B2B",
	"b2c":   "B2C",
	"crm":   "CRM",
	"erp":   "ERP",
	"hr":    "HR",
	"kpi":   "KPI",
	"ppc":   "PPC",
	"pos":   "POS",
	"saas":  "SaaS",
	"seo":   "SEO",
	"spi":   "SPI",
	"smb":   "SMB",
	"smes":  "SMEs",
	"whats": "Whats",
}

func normalizeName(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	raw = nonAlnum.ReplaceAllString(raw, " ")
	return strings.Join(strings.Fields(raw), " ")
}

func deriveDiscoveryCandidateName(payload map[string]any) string {
	if len(payload) == 0 {
		return ""
	}
	for _, key := range []string{"opportunity_name", "vertical_name", "name", "title"} {
		if v := normalizeProvidedVerticalName(asString(payload[key]), false); v != "" {
			return v
		}
	}
	if v := humanizeTaxonomyLabel(firstNonEmptyString(
		asString(payload["subcategory"]),
		asString(payload["trend_category"]),
	)); v != "" {
		return v
	}
	if v := humanizeTaxonomyLabel(asString(payload["category"])); v != "" {
		return v
	}
	for _, key := range []string{"trend_description", "opportunity_hypothesis"} {
		if v := normalizeProvidedVerticalName(asString(payload[key]), true); v != "" {
			return v
		}
	}
	return ""
}

func normalizeProvidedVerticalName(raw string, strictNarrative bool) string {
	name := strings.TrimSpace(raw)
	if name == "" {
		return ""
	}
	name = strings.Join(strings.Fields(name), " ")
	if strictNarrative {
		if len(name) > maxNarrativeFallbackNameLen {
			return ""
		}
		if len(strings.Fields(name)) > maxNarrativeFallbackNameWords {
			return ""
		}
		if punctuationHeavyName.MatchString(name) {
			return ""
		}
	}
	if len(name) > maxVerticalNameLen {
		name = strings.TrimSpace(truncateRunes(name, maxVerticalNameLen))
	}
	// If the candidate looks like a taxonomy token, present a readable label.
	if !strings.Contains(name, " ") && (strings.Contains(name, "_") || strings.Contains(name, "-") || strings.Contains(name, "/")) {
		if humanized := humanizeTaxonomyLabel(name); humanized != "" {
			return humanized
		}
	}
	return name
}

func humanizeTaxonomyLabel(raw string) string {
	norm := normalizeName(raw)
	if norm == "" {
		return ""
	}
	parts := strings.Fields(norm)
	for i, part := range parts {
		if acronym, ok := knownVerticalAcronyms[part]; ok {
			parts[i] = acronym
			continue
		}
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	name := strings.Join(parts, " ")
	if len(name) > maxVerticalNameLen {
		name = strings.TrimSpace(truncateRunes(name, maxVerticalNameLen))
	}
	return name
}

func buildVerticalSlug(name, id string) string {
	base := normalizeName(name)
	base = strings.ReplaceAll(base, " ", "-")
	base = strings.Trim(base, "-")
	if base == "" {
		base = "vertical"
	}
	if len(base) > maxVerticalSlugLen {
		base = strings.Trim(base[:maxVerticalSlugLen], "-")
	}
	if base == "" {
		base = "vertical"
	}
	suffix := id
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	return base + "-" + suffix
}

func fuzzyBestMatch(name string, existing []verticalCandidate) (verticalCandidate, float64) {
	cand := tokenSet(normalizeName(name))
	best := verticalCandidate{}
	bestScore := 0.0
	for _, v := range existing {
		score := jaccard(cand, tokenSet(normalizeName(v.Name)))
		if score > bestScore {
			bestScore = score
			best = v
		}
	}
	return best, bestScore
}

func tokenSet(s string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, t := range strings.Fields(strings.TrimSpace(s)) {
		if t == "" {
			continue
		}
		out[t] = struct{}{}
	}
	return out
}

func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	inter := 0
	union := len(a)
	for k := range b {
		if _, ok := a[k]; ok {
			inter++
		} else {
			union++
		}
	}
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

func truncateRunes(s string, max int) string {
	if max <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

func parsePayloadMap(raw []byte) map[string]any {
	out := map[string]any{}
	if len(raw) == 0 {
		return out
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		runtimeWarn(
			"payload-parse",
			"failed to parse JSON payload bytes=%d error=%v snippet=%q",
			len(raw),
			err,
			snippetForLog(string(raw), 220),
		)
		return map[string]any{}
	}
	if out == nil {
		out = map[string]any{}
	}
	return out
}

func payloadMap(v any) map[string]any {
	return parsePayloadMap(mustJSON(v))
}

func cloneRaw(raw []byte) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	cp := make([]byte, len(raw))
	copy(cp, raw)
	return cp
}

func mergeRawPayload(current, incoming []byte) json.RawMessage {
	base := parsePayloadMap(current)
	next := parsePayloadMap(incoming)
	for k, v := range next {
		base[k] = v
	}
	return mustJSON(base)
}

func asFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	default:
		s := strings.TrimSpace(asString(v))
		if s == "" {
			return 0
		}
		var num json.Number = json.Number(s)
		f, _ := num.Float64()
		return f
	}
}

func payloadIndicatesSynthesisNeeded(payload map[string]any) bool {
	if len(payload) == 0 {
		return false
	}
	for _, key := range []string{"requires_synthesis", "needs_synthesis", "conflict_detected", "conflicting_signals"} {
		if parseBool(payload[key]) {
			return true
		}
	}
	if notes := strings.TrimSpace(asString(payload["conflict_notes"])); notes != "" {
		return true
	}
	return false
}

func parseBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "1", "true", "yes", "y", "on":
			return true
		default:
			return false
		}
	case int:
		return t != 0
	case int64:
		return t != 0
	case float64:
		return t != 0
	default:
		return false
	}
}

func firstNonEmptyString(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func (pc *FactoryPipelineCoordinator) recordTransition(
	ctx context.Context,
	startedAt time.Time,
	eventType string,
	evt events.Event,
	payload map[string]any,
	before map[string]any,
	action string,
	dropReason string,
	emitted []events.Event,
	execErr error,
) {
	if pc == nil || pc.db == nil {
		return
	}
	pipelineType, pipelineID := pc.transitionIdentity(eventType, evt, payload)
	if pipelineID == "" {
		pipelineID = strings.TrimSpace(evt.ID)
	}
	if _, err := uuid.Parse(strings.TrimSpace(pipelineID)); err != nil {
		pipelineID = strings.TrimSpace(evt.ID)
	}
	after := pc.transitionStateSnapshot(eventType, evt, payload)
	emittedTypes := make([]string, 0, len(emitted))
	for _, out := range emitted {
		t := strings.TrimSpace(string(out.Type))
		if t != "" {
			emittedTypes = append(emittedTypes, t)
		}
	}
	errText := strings.TrimSpace(asString(execErr))
	if execErr != nil && errText == "" {
		errText = execErr.Error()
	}
	input := PipelineTransitionInput{
		EventID:       strings.TrimSpace(evt.ID),
		EventType:     eventType,
		Handler:       "pipeline-coordinator",
		PipelineType:  pipelineType,
		PipelineID:    pipelineID,
		Action:        strings.TrimSpace(action),
		StateBefore:   before,
		StateAfter:    after,
		EventsEmitted: emittedTypes,
		DropReason:    strings.TrimSpace(dropReason),
		Error:         errText,
		Duration:      time.Since(startedAt),
	}
	if appendDeferredPipelineTransition(ctx, deferredPipelineTransition{
		db:    pc.db,
		input: input,
	}) {
		return
	}
	_ = RecordPipelineTransition(ctx, pc.db, input)
}

func (pc *FactoryPipelineCoordinator) transitionIdentity(eventType string, evt events.Event, payload map[string]any) (pipelineType string, pipelineID string) {
	if isUUID(strings.TrimSpace(evt.VerticalID)) {
		return "validation", strings.TrimSpace(evt.VerticalID)
	}
	if v := strings.TrimSpace(asString(payload["vertical_id"])); isUUID(v) {
		return "validation", v
	}
	if v := strings.TrimSpace(asString(payload["campaign_id"])); isUUID(v) {
		return "campaign", v
	}
	if v := strings.TrimSpace(asString(payload["directive_id"])); isUUID(v) {
		return "campaign", v
	}
	if v := strings.TrimSpace(asString(payload["scan_id"])); isUUID(v) {
		return "scan", v
	}
	switch {
	case strings.HasPrefix(eventType, "scan."),
		strings.Contains(eventType, ".scan_"),
		eventType == "category.assessed",
		eventType == "trend.identified",
		eventType == "source.scraped",
		eventType == "dedup.resolved",
		eventType == "synthesis.resolved":
		pipelineType = "scan"
	default:
		pipelineType = "validation"
	}
	return pipelineType, strings.TrimSpace(evt.ID)
}

func (pc *FactoryPipelineCoordinator) transitionStateSnapshot(eventType string, evt events.Event, payload map[string]any) map[string]any {
	if pc == nil {
		return nil
	}
	out := map[string]any{
		"event_type":      strings.TrimSpace(eventType),
		"scans_active":    0,
		"scoring_active":  0,
		"pending_dedup":   0,
		"validations":     0,
		"processed_count": 0,
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		verticalID = strings.TrimSpace(asString(payload["vertical_id"]))
	}
	scanID := strings.TrimSpace(asString(payload["scan_id"]))
	if scanID == "" {
		scanID = strings.TrimSpace(evt.ID)
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	out["scans_active"] = len(pc.scans)
	out["scoring_active"] = len(pc.scoring)
	out["pending_dedup"] = len(pc.pendingDedup)
	out["validations"] = len(pc.validations)
	out["processed_count"] = len(pc.processed)
	if verticalID != "" {
		if st := pc.validations[verticalID]; st != nil {
			out["validation_state"] = map[string]any{
				"vertical_id":          st.VerticalID,
				"status":               st.Status,
				"g1_research":          st.G1Research,
				"g2_spec":              st.G2Spec,
				"g3_cto":               st.G3CTO,
				"g4_brand":             st.G4Brand,
				"revision_count":       st.RevisionCount,
				"inner_revision_count": st.InnerRevisionCount,
				"spec_version":         st.SpecVersion,
				"packaging_requested":  st.PackagingRequested,
				"packaging_retries":    st.PackagingRetries,
			}
		}
	}
	if scanID != "" {
		if acc := pc.scans[scanID]; acc != nil {
			out["scan_state"] = map[string]any{
				"scan_id":              acc.ScanID,
				"campaign_id":          acc.CampaignID,
				"mode":                 acc.Mode,
				"geography":            acc.Geography,
				"reports_received":     acc.Reports,
				"expected":             acc.Expected,
				"completed_by":         len(acc.CompletedBy),
				"verticals_discovered": acc.Discovered,
				"verticals_skipped":    acc.Skipped,
				"pending_dedup":        pc.pendingDedupCountForScan(acc.ScanID),
			}
		}
	}
	return out
}

func isUUID(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	_, err := uuid.Parse(raw)
	return err == nil
}

// Snapshot helpers for dashboard/tests.
func (pc *FactoryPipelineCoordinator) SnapshotScans() []map[string]any {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	out := make([]map[string]any, 0, len(pc.scans))
	ids := make([]string, 0, len(pc.scans))
	for id := range pc.scans {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		acc := pc.scans[id]
		out = append(out, map[string]any{
			"scan_id":              acc.ScanID,
			"campaign_id":          acc.CampaignID,
			"mode":                 acc.Mode,
			"geography":            acc.Geography,
			"expected":             acc.Expected,
			"completed":            len(acc.CompletedBy),
			"reports":              acc.Reports,
			"verticals_discovered": acc.Discovered,
			"verticals_skipped":    acc.Skipped,
		})
	}
	return out
}
