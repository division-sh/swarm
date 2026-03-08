package pipeline

type scoreDimensionResult = ScoreDimensionResult
type contestedDimension = ContestedDimension
type scoringComposite = ScoringComposite
type scoringAccumulatorInput = ScoringAccumulatorInput
type validationContextSnapshot = ValidationContextSnapshot
type scanCompletedBuildInput = ScanCompletedBuildInput

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

type ScanCompletedBuildInput struct {
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
	ShardsTotal     int
	ShardsCompleted int
	ShardsFailed    int
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

type ValidationContextSnapshot struct {
	Research    map[string]any
	Spec        map[string]any
	CTONotes    map[string]any
	Brand       map[string]any
	Scoring     map[string]any
	SpecVersion int
}

type ScoreDimensionResult struct {
	Score      int    `json:"score"`
	Evidence   string `json:"evidence"`
	Confidence string `json:"confidence,omitempty"`
}

type ContestedDimension struct {
	Dimension string                 `json:"dimension"`
	Scores    []int                  `json:"scores"`
	Evidence  []string               `json:"evidence"`
	Spread    int                    `json:"spread"`
	Options   []ScoreDimensionResult `json:"options,omitempty"`
}

type ScoringComposite struct {
	Result         string                          `json:"result"`
	Reason         string                          `json:"reason"`
	CompositeScore float64                         `json:"composite_score"`
	ViabilityScore float64                         `json:"viability_score"`
	MarketScore    float64                         `json:"market_score"`
	Dimensions     map[string]ScoreDimensionResult `json:"dimensions"`
	Rubric         string                          `json:"rubric,omitempty"`
	Partial        bool                            `json:"partial"`
}

type ScoringAccumulatorInput struct {
	Rubric   string
	Expected []string
	Received map[string]ScoreDimensionResult
	Partial  bool
}

type VerticalScoredPayload struct {
	VerticalID     string                          `json:"vertical_id"`
	Result         string                          `json:"result,omitempty"`
	Reason         string                          `json:"reason,omitempty"`
	CompositeScore float64                         `json:"composite_score"`
	ViabilityScore float64                         `json:"viability_score"`
	MarketScore    float64                         `json:"market_score"`
	Dimensions     map[string]ScoreDimensionResult `json:"dimensions"`
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
	Dimensions        map[string]ScoreDimensionResult `json:"dimensions"`
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

type DiscoveryPolicy interface {
	EvaluateDiscoveryPreFilter(payload map[string]any, rawSignal float64) (bool, float64, string)
	BuildPrefilterSkipDetail(payload map[string]any, rawSignal, adjustedSignal float64, reason, mode string) map[string]any
	NormalizeOpportunityPattern(raw string) string
}

type ScoringPolicy interface {
	ExpectedScoringDimensions(rubric string) []string
	SelectScoringRubric(mode string) string
	ComputeComposite(in ScoringAccumulatorInput) ScoringComposite
	BuildDiscoveryContextPayload(raw map[string]any) map[string]any
	ResolveScoringAnalysisRecipient(recipients []string, excludedAgent string) string
	NormalizeGeographicScope(raw string) string
}

type PayloadFactory interface {
	BuildScanAssignedPayload(scanID, campaignID, mode, geography string, source map[string]any, plannedShards int) ScanAssignedPayload
	BuildSynthesisNeededPayload(scanID, campaignID, mode, geography string, raw map[string]any) SynthesisNeededPayload
	BuildDedupAmbiguousPayload(scanID, dedupEventID string, similarity float64, candidateName, geography string, signal float64, existingID, existingName string) DedupAmbiguousPayload
	BuildVerticalDiscoveredPayload(verticalID, name, geography, mode, scanID, campaignID string, signal float64, discoverySource string, rawSignals map[string]any) VerticalDiscoveredPayload
	BuildScanCompletedPayload(in ScanCompletedBuildInput) ScanCompletedPayload
	BuildScoringRequestedPayload(verticalID, verticalName, geography, mode, rubric string, dimensions []string, discoveryContext map[string]any) ScoringRequestedPayload
	BuildScoringContestedPayload(verticalID, dimension string, contest ContestedDimension, rubric, mode string) ScoringContestedPayload
	BuildVerticalScoredPayload(verticalID string, result ScoringComposite, verticalName, geography, mode string) VerticalScoredPayload
	BuildVerticalShortlistedPayload(verticalID string, composite, viability float64, scoringPayload map[string]any) VerticalShortlistedPayload
	BuildVerticalMarginalPayload(verticalID string, result ScoringComposite) VerticalMarginalPayload
	BuildVerticalRejectedPayload(verticalID string, result ScoringComposite) VerticalRejectedPayload

	BuildBrandRequestedPayload(verticalID, name, geography string, scoring, brief map[string]any) BrandRequestedPayload
	BuildValidationPackageReadyPayload(verticalID, name, geography string, snap ValidationContextSnapshot) ValidationPackageReadyPayload
	BuildSpecValidationRequestedPayload(verticalID string, spec map[string]any) SpecValidationRequestedPayload
	BuildCTOSpecReviewRequestedPayload(verticalID, name, geography string, specValidation map[string]any, snap ValidationContextSnapshot) CTOSpecReviewRequestedPayload
	BuildSpecRevisionRequestedPayload(verticalID, source, name, geography string, feedback map[string]any, snap ValidationContextSnapshot) SpecRevisionRequestedPayload
	BuildValidationMoreDataPayload(verticalID, name, geography string, request map[string]any, snap ValidationContextSnapshot) ValidationMoreDataNeededPayload
	BuildBrandRevisionNeededPayload(verticalID, name, geography string, feedback, brand map[string]any) BrandRevisionNeededPayload
	BuildVerticalKilledPayload(verticalID, name, geography, sourceEvent string, reason map[string]any) VerticalKilledPayload
	BuildValidationStartedPayload(verticalID, name, geography string, scoring map[string]any) ValidationStartedPayload
}

type WorkflowModule interface {
	DiscoveryPolicy() DiscoveryPolicy
	ScoringPolicy() ScoringPolicy
	PayloadFactory() PayloadFactory
}

var defaultWorkflowModuleFactory func() WorkflowModule

func SetDefaultWorkflowModuleFactory(factory func() WorkflowModule) {
	defaultWorkflowModuleFactory = factory
}
