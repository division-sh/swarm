package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
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
)

type pipelineEmitCollectorKey struct{}

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
}

type scanAccumulator struct {
	ScanID      string
	CampaignID  string
	Mode        string
	Geography   string
	Expected    int
	CompletedBy map[string]struct{}
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
	VerticalID      string
	VerticalName    string
	Geography       string
	Mode            string
	Rubric          string
	Expected        []string
	Received        map[string]scoreDimensionResult
	Contested       map[string]contestedDimension
	RequestedAt     time.Time
	LastUpdatedAt   time.Time
	ContestNotified map[string]bool
}

type pendingCandidate struct {
	DedupEventID string
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
	VerticalID     string         `json:"vertical_id"`
	VerticalName   string         `json:"vertical_name,omitempty"`
	Geography      string         `json:"geography,omitempty"`
	Spec           map[string]any `json:"spec"`
	SpecVersion    int            `json:"spec_version"`
	ValidationTier string         `json:"validation_tier"`
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
		"vertical.needs_more_data",
		"brand.revision_needed":
		return "status=" + status + ", expected=active"
	default:
		return ""
	}
}

func (pc *FactoryPipelineCoordinator) interceptPolicy(eventType string, evt events.Event) (consume bool, handled bool) {
	switch eventType {
	case "scan.requested",
		"vertical.discovered",
		"score.dimension_complete",
		"scoring.contest_resolved",
		"category.assessed",
		"trend.identified",
		"source.scraped",
		"market_research.scan_complete",
		"trend_research.scan_complete",
		"scanner.google_maps.scan_complete",
		"scanner.instagram.scan_complete",
		"scanner.reviews.scan_complete",
		"scanner.directories.scan_complete",
		"scanner.job_boards.scan_complete",
		"dedup.resolved",
		"synthesis.resolved",
		"vertical.shortlisted",
		"research.completed",
		"research.vertical_rejected",
		"spec.revision_requested",
		"spec.approved",
		"cto.spec_approved",
		"cto.spec_revision_needed",
		"cto.spec_vetoed",
		"brand.candidates_ready",
		"vertical.needs_more_data",
		"brand.revision_needed",
		"vertical.resumed":
		return true, true
	case "spec.validation_passed", "spec.validation_failed":
		if strings.TrimSpace(evt.VerticalID) == "" {
			return false, false
		}
		return true, true
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
		events.EventType("scan.requested"),
		events.EventType("vertical.discovered"),
		events.EventType("score.dimension_complete"),
		events.EventType("scoring.contest_resolved"),
		events.EventType("category.assessed"),
		events.EventType("trend.identified"),
		events.EventType("source.scraped"),
		events.EventType("market_research.scan_complete"),
		events.EventType("trend_research.scan_complete"),
		events.EventType("scanner.google_maps.scan_complete"),
		events.EventType("scanner.instagram.scan_complete"),
		events.EventType("scanner.reviews.scan_complete"),
		events.EventType("scanner.directories.scan_complete"),
		events.EventType("scanner.job_boards.scan_complete"),
		events.EventType("dedup.resolved"),
		events.EventType("synthesis.resolved"),
		events.EventType("vertical.shortlisted"),
		events.EventType("research.completed"),
		events.EventType("research.vertical_rejected"),
		events.EventType("spec.revision_requested"),
		events.EventType("spec.approved"),
		events.EventType("spec.validation_passed"),
		events.EventType("spec.validation_failed"),
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
	case "scan.requested":
		pc.handleScanRequested(ctx, evt)
	case "vertical.discovered":
		pc.handleScoringRequested(ctx, evt)
	case "score.dimension_complete":
		pc.handleScoreDimensionComplete(ctx, evt)
	case "scoring.contest_resolved":
		pc.handleScoringContestResolved(ctx, evt)
	case "category.assessed", "trend.identified", "source.scraped":
		pc.handleDiscoveryReport(ctx, evt)
	case "market_research.scan_complete", "trend_research.scan_complete",
		"scanner.google_maps.scan_complete", "scanner.instagram.scan_complete",
		"scanner.reviews.scan_complete", "scanner.directories.scan_complete",
		"scanner.job_boards.scan_complete":
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
	case "cto.spec_revision_needed":
		pc.handleCTORevisionNeeded(ctx, evt)
	case "research.vertical_rejected", "cto.spec_vetoed":
		pc.handleValidationRejected(ctx, evt)
	case "vertical.ready_for_review":
		pc.handleValidationPackaged(evt)
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
		SELECT scan_id::text, COALESCE(campaign_id::text,''), mode, geography,
		       expected, COALESCE(completed_by, '{}'::jsonb), reports, discovered, skipped, created_at
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
			var completed map[string]bool
			_ = json.Unmarshal(completedRaw, &completed)
			for key, done := range completed {
				if done {
					completedBy[key] = struct{}{}
				}
			}
			scans[scanID] = &scanAccumulator{
				ScanID:      scanID,
				CampaignID:  campaignID,
				Mode:        mode,
				Geography:   geography,
				Expected:    expected,
				CompletedBy: completedBy,
				Reports:     reports,
				Discovered:  discovered,
				Skipped:     skipped,
				CreatedAt:   createdAt,
			}
		}
		_ = scanRows.Close()
	}

	pendingRows, err := dbQueryContext(ctx, pc.db, `
		SELECT dedup_event_id::text, scan_id::text, COALESCE(campaign_id::text,''), mode,
		       geography, name, signal_strength, COALESCE(payload, '{}'::jsonb)
		FROM pending_dedup_candidates
	`)
	if err == nil {
		for pendingRows.Next() {
			var (
				dedupID, scanID, campaignID, mode, geography, name string
				signal                                             float64
				payloadRaw                                         []byte
			)
			if scanErr := pendingRows.Scan(&dedupID, &scanID, &campaignID, &mode, &geography, &name, &signal, &payloadRaw); scanErr != nil {
				continue
			}
			pending[dedupID] = pendingCandidate{
				DedupEventID: dedupID,
				ScanID:       scanID,
				CampaignID:   campaignID,
				Mode:         mode,
				Geography:    geography,
				Name:         name,
				Signal:       signal,
				Payload:      parsePayloadMap(payloadRaw),
			}
		}
		_ = pendingRows.Close()
	}

	validationRows, err := dbQueryContext(ctx, pc.db, `
		SELECT vertical_id::text, status, g1_research, g2_spec, g3_cto, g4_brand,
		       COALESCE(research_payload, '{}'::jsonb), COALESCE(spec_payload, '{}'::jsonb),
		       COALESCE(cto_payload, '{}'::jsonb), COALESCE(brand_payload, '{}'::jsonb),
		       COALESCE(scoring_payload, '{}'::jsonb), revision_count, inner_revision_count,
		       spec_version, packaging_requested, packaging_requested_at, packaging_retries
		FROM validation_pipelines
	`)
	if err == nil {
		for validationRows.Next() {
			var (
				verticalID, status                                                     string
				g1, g2, g3, g4                                                         bool
				researchPayload, specPayload, ctoPayload, brandPayload, scoringPayload []byte
				revisionCount, innerRevisionCount, specVersion, packagingRetries       int
				packagingRequested                                                     bool
				packagingRequestedAt                                                   sql.NullTime
			)
			if scanErr := validationRows.Scan(
				&verticalID, &status, &g1, &g2, &g3, &g4,
				&researchPayload, &specPayload, &ctoPayload, &brandPayload, &scoringPayload,
				&revisionCount, &innerRevisionCount, &specVersion, &packagingRequested, &packagingRequestedAt, &packagingRetries,
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
				PackagingRequested:   packagingRequested,
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
	defer pc.mu.Unlock()
	if pc.stateLoaded {
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
		completedBy := map[string]bool{}
		for key := range acc.CompletedBy {
			completedBy[key] = true
		}
		_, _ = dbExecContext(ctx, pc.db, `
			INSERT INTO scan_accumulators (
				scan_id, campaign_id, mode, geography, expected,
				completed_by, reports, discovered, skipped, created_at, updated_at
			)
			VALUES (
				$1, NULLIF($2,''), $3, $4, $5,
				$6::jsonb, $7, $8, $9, $10, now()
			)
			ON CONFLICT (scan_id) DO UPDATE SET
				campaign_id = EXCLUDED.campaign_id,
				mode = EXCLUDED.mode,
				geography = EXCLUDED.geography,
				expected = EXCLUDED.expected,
				completed_by = EXCLUDED.completed_by,
				reports = EXCLUDED.reports,
				discovered = EXCLUDED.discovered,
				skipped = EXCLUDED.skipped,
				updated_at = now()
		`, acc.ScanID, acc.CampaignID, acc.Mode, acc.Geography, acc.Expected, string(mustJSON(completedBy)), acc.Reports, acc.Discovered, acc.Skipped, acc.CreatedAt)
	}

	for _, cand := range pc.pendingDedup {
		_, _ = dbExecContext(ctx, pc.db, `
			INSERT INTO pending_dedup_candidates (
				dedup_event_id, scan_id, campaign_id, mode, geography, name, signal_strength, payload, created_at
			)
			VALUES (
				$1, $2, NULLIF($3,''), $4, $5, $6, $7, $8::jsonb, now()
			)
			ON CONFLICT (dedup_event_id) DO UPDATE SET
				scan_id = EXCLUDED.scan_id,
				campaign_id = EXCLUDED.campaign_id,
				mode = EXCLUDED.mode,
				geography = EXCLUDED.geography,
				name = EXCLUDED.name,
				signal_strength = EXCLUDED.signal_strength,
				payload = EXCLUDED.payload
		`, cand.DedupEventID, cand.ScanID, cand.CampaignID, cand.Mode, cand.Geography, cand.Name, cand.Signal, string(mustJSON(cand.Payload)))
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
				research_payload, spec_payload, cto_payload, brand_payload, scoring_payload,
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
		CreatedAt:   time.Now(),
	}
	if plannedShardCount > 0 {
		acc.Expected = plannedShardCount
	}
	pc.mu.Lock()
	pc.scans[scanID] = acc
	pc.mu.Unlock()

	base := map[string]any{
		"scan_id":             scanID,
		"campaign_id":         campaignID,
		"mode":                mode,
		"geography":           geography,
		"geography_id":        strings.TrimSpace(asString(payload["geography_id"])),
		"taxonomy_categories": payload["taxonomy_categories"],
		"priority":            strings.TrimSpace(asString(payload["priority"])),
		"campaign_context":    payload["campaign_context"],
		"directive_id":        strings.TrimSpace(asString(payload["directive_id"])),
		"strategic_context":   payload["strategic_context"],
		"requested_at":        time.Now().UTC().Format(time.RFC3339),
		"planned_shards":      plannedShardCount,
	}
	if plannedShardCount > 0 && (mode == "saas_gap" || mode == "saas_trend") {
		// Assignment dispatch is owned by the shard dispatcher loop.
		return
	}

	switch mode {
	case "saas_gap":
		pc.publish(ctx, "market_research.scan_assigned", "", base)
	case "saas_trend":
		pc.publish(ctx, "trend_research.scan_assigned", "", base)
	case "local_services":
		// Phase 1-3 synthetic adapter: single scanner path.
		pc.publish(ctx, "scanner.google_maps.scan_assigned", "", base)
	default:
		pc.publish(ctx, "market_research.scan_assigned", "", base)
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
			CreatedAt:   time.Now(),
		}
		if acc.Mode == "" {
			acc.Mode = "saas_gap"
		}
		pc.scans[scanID] = acc
	}
	acc.Reports++
	pc.mu.Unlock()

	if payloadIndicatesSynthesisNeeded(payload) {
		pc.publish(ctx, "synthesis.needed", "", map[string]any{
			"scan_id":        scanID,
			"campaign_id":    acc.CampaignID,
			"mode":           acc.Mode,
			"geography":      firstNonEmptyString(strings.TrimSpace(asString(payload["geography"])), acc.Geography),
			"category":       strings.TrimSpace(asString(payload["category"])),
			"subcategory":    strings.TrimSpace(asString(payload["subcategory"])),
			"conflict_notes": payload["conflict_notes"],
			"raw_report":     payload,
		})
		return
	}

	signal := asFloat(payload["signal_strength"])
	if signal < 50 {
		pc.mu.Lock()
		acc.Skipped++
		pc.mu.Unlock()
		return
	}

	name := deriveDiscoveryCandidateName(payload)
	if name == "" {
		runtimeWarn(
			"pipeline-coordinator",
			"skipping discovery candidate with missing name scan_id=%s event_id=%s source=%s",
			scanID,
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(evt.SourceAgent),
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
			ScanID:       scanID,
			CampaignID:   acc.CampaignID,
			Mode:         acc.Mode,
			Geography:    geography,
			Name:         name,
			Signal:       signal,
			Payload:      payload,
		}
		pc.mu.Lock()
		pc.pendingDedup[dedupEventID] = cand
		pc.mu.Unlock()
		pc.publish(ctx, "dedup.ambiguous", "", map[string]any{
			"scan_id":           scanID,
			"dedup_event_id":    dedupEventID,
			"similarity":        score,
			"new_candidate":     map[string]any{"name": name, "geography": geography, "signal_strength": signal},
			"existing_vertical": map[string]any{"id": best.ID, "name": best.Name, "geography": geography},
		})
		return
	}

	verticalID, err := pc.ensureVerticalDiscovered(ctx, name, geography, acc.Mode, payload)
	if err != nil {
		log.Printf("pipeline: ensure discovered vertical failed name=%s geo=%s err=%v", name, geography, err)
		return
	}
	pc.mu.Lock()
	acc.Discovered++
	pc.mu.Unlock()
	pc.publish(ctx, "vertical.discovered", verticalID, map[string]any{
		"vertical_id":      verticalID,
		"name":             name,
		"geography":        geography,
		"mode":             acc.Mode,
		"scan_id":          scanID,
		"campaign_id":      acc.CampaignID,
		"signal_strength":  signal,
		"discovery_source": evt.SourceAgent,
		"raw_signals":      payload,
	})
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
	pc.publish(ctx, "vertical.discovered", verticalID, map[string]any{
		"vertical_id":      verticalID,
		"name":             cand.Name,
		"geography":        cand.Geography,
		"mode":             cand.Mode,
		"scan_id":          cand.ScanID,
		"campaign_id":      cand.CampaignID,
		"signal_strength":  cand.Signal,
		"discovery_source": "pipeline-coordinator",
		"raw_signals":      cand.Payload,
	})
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
	stats := map[string]any{
		"scan_id":              acc.ScanID,
		"campaign_id":          acc.CampaignID,
		"mode":                 acc.Mode,
		"geography":            acc.Geography,
		"reports_received":     acc.Reports,
		"agents_expected":      maxInt(acc.Expected, 1),
		"agents_complete":      len(acc.CompletedBy),
		"verticals_discovered": acc.Discovered,
		"verticals_skipped":    acc.Skipped,
		"pending_dedup":        pc.pendingDedupCountForScan(acc.ScanID),
		"timed_out":            false,
	}
	if hasShardProgress {
		terminal := shardCompleted + shardFailed
		stats["agents_expected"] = shardTotal
		stats["agents_complete"] = terminal
		stats["shards_total"] = shardTotal
		stats["shards_completed"] = shardCompleted
		stats["shards_failed"] = shardFailed
		done = terminal >= shardTotal && shardTotal > 0
	}
	if done {
		delete(pc.scans, scanID)
	}
	pc.mu.Unlock()

	if done {
		pc.publish(ctx, "scan.completed", "", stats)
	}
}

var rubricWeights = map[string]map[string]float64{
	"local_services": {
		"willingness_to_pay":   0.20,
		"retention_likelihood": 0.15,
		"channel_access":       0.15,
		"operational_friction": 0.10,
		"business_density":     0.12,
		"pain_severity":        0.10,
		"competition_weakness": 0.10,
		"revenue_per_business": 0.08,
	},
	"saas": {
		"willingness_to_pay":     0.15,
		"retention_likelihood":   0.15,
		"technical_feasibility":  0.15,
		"distribution_access":    0.15,
		"regulatory_moat":        0.12,
		"competition_weakness":   0.10,
		"pain_severity":          0.08,
		"market_size":            0.05,
		"localization_advantage": 0.05,
	},
}

var viabilityDimensions = map[string][]string{
	"local_services": {"willingness_to_pay", "retention_likelihood", "channel_access", "operational_friction"},
	"saas":           {"willingness_to_pay", "retention_likelihood", "technical_feasibility", "distribution_access"},
}

func expectedScoringDimensions(rubric string) []string {
	weights := rubricWeights[strings.TrimSpace(rubric)]
	if len(weights) == 0 {
		return nil
	}
	out := make([]string, 0, len(weights))
	for dim := range weights {
		out = append(out, dim)
	}
	sort.Strings(out)
	return out
}

func selectScoringRubric(mode string) string {
	switch normalizeScanMode(mode) {
	case "local_services":
		return "local_services"
	default:
		return "saas"
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
	pc.mu.Lock()
	acc := &scoringAccumulator{
		VerticalID:      verticalID,
		VerticalName:    name,
		Geography:       geography,
		Mode:            mode,
		Rubric:          rubric,
		Expected:        expected,
		Received:        make(map[string]scoreDimensionResult, len(expected)),
		Contested:       make(map[string]contestedDimension),
		RequestedAt:     now,
		LastUpdatedAt:   now,
		ContestNotified: make(map[string]bool),
	}
	if existing := pc.scoring[verticalID]; existing != nil {
		// Keep existing progress but refresh metadata when discovery details improve.
		acc = existing
		acc.VerticalName = firstNonEmptyString(name, acc.VerticalName)
		acc.Geography = firstNonEmptyString(geography, acc.Geography)
		acc.Mode = mode
		acc.Rubric = rubric
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

	pc.publish(ctx, "scoring.requested", verticalID, map[string]any{
		"vertical_id":          verticalID,
		"vertical_name":        acc.VerticalName,
		"geography":            acc.Geography,
		"mode":                 acc.Mode,
		"rubric":               acc.Rubric,
		"dimensions_requested": acc.Expected,
	})
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
			Rubric:          "saas",
			Expected:        expectedScoringDimensions("saas"),
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
			pc.publish(ctx, "scoring.contested", verticalID, map[string]any{
				"vertical_id": verticalID,
				"dimension":   dim,
				"scores":      contest.Scores,
				"evidence":    contest.Evidence,
				"spread":      contest.Spread,
				"rubric":      acc.Rubric,
				"mode":        acc.Mode,
			})
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
		weights = rubricWeights["saas"]
	}
	viabilitySet := viabilityDimensions[acc.Rubric]
	if len(viabilitySet) == 0 {
		viabilitySet = viabilityDimensions["saas"]
	}
	dimensions := make(map[string]scoreDimensionResult, len(acc.Expected))
	viabilitySum := 0.0
	viabilityWeight := 0.0
	marketSum := 0.0
	marketWeight := 0.0

	for _, dim := range acc.Expected {
		res, ok := acc.Received[dim]
		if !ok {
			res = scoreDimensionResult{Score: 0, Evidence: "missing_dimension_timeout"}
		}
		dimensions[dim] = res
		w := weights[dim]
		if w <= 0 {
			continue
		}
		if dimensionInSet(viabilitySet, dim) {
			viabilitySum += float64(res.Score) * w
			viabilityWeight += w
		} else {
			marketSum += float64(res.Score) * w
			marketWeight += w
		}
	}

	viability := 0.0
	market := 0.0
	if viabilityWeight > 0 {
		viability = viabilitySum / viabilityWeight
	}
	if marketWeight > 0 {
		market = marketSum / marketWeight
	}
	composite := viability*0.60 + market*0.40

	out := scoringComposite{
		Result:         "marginal",
		CompositeScore: composite,
		ViabilityScore: viability,
		MarketScore:    market,
		Dimensions:     dimensions,
		Rubric:         acc.Rubric,
		Partial:        partial,
	}
	switch {
	case viability < 65:
		out.Result = "rejected"
		out.Reason = "viability_floor"
	case composite >= 75:
		out.Result = "shortlisted"
	case composite >= 50:
		out.Result = "marginal"
	default:
		out.Result = "rejected"
		out.Reason = "composite_below_50"
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

	scoredPayload := map[string]any{
		"vertical_id":     verticalID,
		"result":          result.Result,
		"reason":          result.Reason,
		"composite_score": result.CompositeScore,
		"viability_score": result.ViabilityScore,
		"market_score":    result.MarketScore,
		"dimensions":      result.Dimensions,
		"rubric":          result.Rubric,
		"partial":         result.Partial,
		"mode":            acc.Mode,
		"vertical_name":   acc.VerticalName,
		"geography":       acc.Geography,
	}
	pc.publish(ctx, "vertical.scored", verticalID, scoredPayload)

	stage := "marginal_review"
	switch result.Result {
	case "shortlisted":
		stage = "shortlisted"
		pc.publish(ctx, "vertical.shortlisted", verticalID, map[string]any{
			"vertical_id":     verticalID,
			"composite_score": result.CompositeScore,
			"viability_score": result.ViabilityScore,
			"scoring_payload": scoredPayload,
		})
		// Runtime-emitted shortlist must also start validation immediately,
		// because deferred events bypass interceptor re-entry.
		pc.handleValidationStarted(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.shortlisted"),
			SourceAgent: "pipeline-coordinator",
			VerticalID:  verticalID,
			Payload:     mustJSON(map[string]any{"scoring_payload": scoredPayload}),
			CreatedAt:   time.Now().UTC(),
		})
	case "marginal":
		pc.publish(ctx, "vertical.marginal", verticalID, map[string]any{
			"vertical_id":        verticalID,
			"composite_score":    result.CompositeScore,
			"viability_score":    result.ViabilityScore,
			"dimensions":         result.Dimensions,
			"promotion_eligible": result.ViabilityScore >= 65,
		})
	case "rejected":
		stage = "killed"
		pc.publish(ctx, "vertical.rejected", verticalID, map[string]any{
			"vertical_id": verticalID,
			"reason": map[string]any{
				"code":            result.Reason,
				"composite_score": result.CompositeScore,
				"viability_score": result.ViabilityScore,
			},
		})
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
		`, verticalID, stage, string(mustJSON(scoredPayload)), strings.TrimSpace(result.Reason)); err != nil {
			log.Printf("pipeline: update vertical score state failed vertical=%s err=%v", verticalID, err)
		}
	}
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
	specVersion := st.SpecVersion
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

	if gate == "g2" {
		pc.publish(ctx, "spec.validation_requested", verticalID, payloadMap(pc.buildSpecValidationRequestedPayload(ctx, verticalID, payload, specVersion)))
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
	pc.publish(ctx, "vertical.killed", verticalID, payloadMap(pc.buildVerticalKilledPayload(ctx, verticalID, string(evt.Type), parsePayloadMap(evt.Payload))))
}

func (pc *FactoryPipelineCoordinator) handleValidationPackaged(evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	st := pc.getValidationStateLocked(verticalID)
	st.Status = "packaged"
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
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
		stats := map[string]any{
			"scan_id":              scan.scanID,
			"campaign_id":          scan.campaignID,
			"mode":                 scan.mode,
			"geography":            scan.geography,
			"reports_received":     scan.reports,
			"agents_expected":      scan.expected,
			"agents_complete":      scan.completed,
			"verticals_discovered": scan.discovered,
			"verticals_skipped":    scan.skipped,
			"pending_dedup":        scan.pendingDedup,
			"timed_out":            true,
		}
		shardTotal, shardCompleted, shardFailed, hasShardProgress := pc.shardTerminalProgress(ctx, scan.shardScanID)
		if hasShardProgress {
			terminal := shardCompleted + shardFailed
			stats["agents_expected"] = shardTotal
			stats["agents_complete"] = terminal
			stats["shards_total"] = shardTotal
			stats["shards_completed"] = shardCompleted
			stats["shards_failed"] = shardFailed
		}
		pc.publish(ctx, "scan.completed", "", stats)
	}
}

func (pc *FactoryPipelineCoordinator) publish(ctx context.Context, eventType, verticalID string, payload map[string]any) {
	if pc == nil {
		return
	}
	if payload == nil {
		payload = map[string]any{}
	}
	emitted := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: "pipeline-coordinator",
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

func (pc *FactoryPipelineCoordinator) buildSpecValidationRequestedPayload(ctx context.Context, verticalID string, spec map[string]any, specVersion int) SpecValidationRequestedPayload {
	if spec == nil {
		spec = map[string]any{}
	}
	name, geography := pc.identityForPayload(ctx, verticalID)
	return SpecValidationRequestedPayload{
		VerticalID:     strings.TrimSpace(verticalID),
		VerticalName:   name,
		Geography:      geography,
		Spec:           spec,
		SpecVersion:    specVersion,
		ValidationTier: "vertical_spec",
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
	return verticalID, nil
}

func expectedAgents(mode string) int {
	switch normalizeScanMode(mode) {
	case "saas_gap", "saas_trend":
		return 1
	case "local_services":
		// Phase 1-3 synthetic adapter mode runs through one scanner adapter.
		return 1
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
	for _, key := range []string{"vertical_name", "name", "title"} {
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
