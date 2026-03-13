package pipeline

import (
	"context"

	"empireai/internal/events"
	"empireai/internal/runtime/semanticview"
)

type scoreDimensionResult = ScoreDimensionResult
type contestedDimension = ContestedDimension
type scoringComposite = ScoringComposite
type scoringAccumulatorInput = ScoringAccumulatorInput
type ScoringAccumulator = scoringAccumulator
type BackgroundNode interface{ Run(context.Context) }
type validationContextSnapshot = ValidationContextSnapshot
type scanCompletedBuildInput = ScanCompletedBuildInput

type ValidationContextSnapshot struct {
	Research    map[string]any
	Spec        map[string]any
	CTONotes    map[string]any
	Brand       map[string]any
	Scoring     map[string]any
	SpecVersion int
}

type ScoreDimensionResult struct {
	Score      int
	Evidence   string
	Confidence string
}

type ContestedDimension struct {
	Dimension string
	Scores    []int
	Evidence  []string
	Spread    int
	Options   []ScoreDimensionResult
}

type ScoringComposite struct {
	Result         string
	Reason         string
	CompositeScore float64
	ViabilityScore float64
	MarketScore    float64
	Dimensions     map[string]ScoreDimensionResult
	Rubric         string
	Partial        bool
}

type ScoringAccumulatorInput struct {
	Rubric    string
	Expected  []string
	Received  map[string]ScoreDimensionResult
	Contested map[string]ContestedDimension
	Partial   bool
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

type PortfolioDigestTimerPayload struct {
	Message                   string
	DigestText                string
	TriggerReason             string
	Snapshot                  map[string]any
	Metadata                  map[string]any
	VerticalID                string
	TaskID                    string
	RecentRejections          []map[string]any
	RejectionCount            int
	ScoringRejectionsInjected bool
	ScoringRejectionsCount    int
	ScoringRejectionSummaries []map[string]any
}

type DiscoveryPolicy interface {
	EvaluateDiscoveryPreFilter(payload map[string]any, rawSignal float64) (bool, float64, string)
	BuildPrefilterSkipDetail(payload map[string]any, rawSignal, adjustedSignal float64, reason, mode string) map[string]any
	NormalizeOpportunityPattern(raw string) string
	BuildDiscoveryCandidatesForReport(scanMode string, payload map[string]any) []DiscoveryCandidate
}

type DiscoveryCandidate struct {
	Mode    string
	Signal  float64
	Payload map[string]any
}

type ScanPolicy interface {
	ExpandScanAssignments(mode string, payload map[string]any, assigned map[string]any, batchSize int) ([]map[string]any, error)
	ReadJSONLBatches(path string, batchSize int) ([][]map[string]any, error)
	ParseDirective(text string) ParsedDirective
	ParseDirectiveGeography(text string) (name, country, region string)
	SanitizeGeographyPhrase(text string) string
	IsComplexDirectiveText(text string) bool
	ResolveDirectiveCorpusPath(mode string, parsed ParsedDirective, payload map[string]any) (string, error)
	ExtractCorpusPathFromStrategicContext(strategic map[string]any) string
}

type ScoringPolicy interface {
	ExpectedScoringDimensions(rubric string) []string
	SelectScoringRubric(mode string) string
	ComputeComposite(in ScoringAccumulatorInput) ScoringComposite
	BuildDiscoveryContextPayload(raw map[string]any) map[string]any
	ResolveScoringAnalysisRecipient(recipients []string, excludedAgent string) string
	NormalizeGeographicScope(raw string) string
	ScoringRestoreDeltaBucket() string
	EncodeScoringRestoreDelta(acc *ScoringAccumulator) map[string]any
	ApplyScoringRestoreDelta(acc *ScoringAccumulator, delta map[string]any)
}

type PayloadFactory interface {
	BuildScanAssignedPayload(scanID, campaignID, mode, geography string, source map[string]any, plannedShards int) map[string]any
	BuildSynthesisNeededPayload(scanID, campaignID, mode, geography string, raw map[string]any) map[string]any
	BuildDedupAmbiguousPayload(scanID, dedupEventID string, similarity float64, candidateName, geography string, signal float64, existingID, existingName string) map[string]any
	BuildVerticalDiscoveredPayload(verticalID, name, geography, mode, scanID, campaignID string, signal float64, discoverySource string, rawSignals map[string]any) map[string]any
	BuildScanCompletedPayload(in ScanCompletedBuildInput) map[string]any
	BuildScoringRequestedPayload(verticalID, verticalName, geography, mode, rubric string, dimensions []string, discoveryContext map[string]any) map[string]any
	BuildScoringContestedPayload(verticalID, dimension string, contest ContestedDimension, rubric, mode string) map[string]any
	BuildVerticalScoredPayload(verticalID string, result ScoringComposite, verticalName, geography, mode string) map[string]any
	BuildVerticalShortlistedPayload(verticalID string, composite, viability float64, scoringPayload map[string]any) map[string]any
	BuildVerticalMarginalPayload(verticalID string, result ScoringComposite) map[string]any
	BuildVerticalRejectedPayload(verticalID string, result ScoringComposite) map[string]any

	BuildBrandRequestedPayload(verticalID, name, geography string, scoring, brief map[string]any) map[string]any
	BuildValidationPackageReadyPayload(verticalID, name, geography string, snap ValidationContextSnapshot) map[string]any
	BuildSpecValidationRequestedPayload(verticalID string, spec map[string]any) map[string]any
	BuildCTOSpecReviewRequestedPayload(verticalID, name, geography string, specValidation map[string]any, snap ValidationContextSnapshot) map[string]any
	BuildSpecRevisionRequestedPayload(verticalID, source, name, geography string, feedback map[string]any, snap ValidationContextSnapshot) map[string]any
	BuildValidationMoreDataPayload(verticalID, name, geography string, request map[string]any, snap ValidationContextSnapshot) map[string]any
	BuildBrandRevisionNeededPayload(verticalID, name, geography string, feedback, brand map[string]any) map[string]any
	BuildVerticalKilledPayload(verticalID, name, geography, sourceEvent string, reason map[string]any) map[string]any
	BuildValidationStartedPayload(verticalID, name, geography string, scoring map[string]any) map[string]any
}

type WorkflowHookContext struct {
	Event      events.Event
	EntityID   string
	Payload    map[string]any
	State      WorkflowState
}

type WorkflowHookRuntime interface {
	ContractPolicyFloat(key string, fallback float64) float64
	ContractPolicyInt(key string, fallback int) int
	PipelineHasCapacity(ctx context.Context, limit int) bool
	PublishWorkflowEvent(ctx context.Context, eventType, verticalID string, payload map[string]any)
	PersistWorkflowMetadata(ctx context.Context, verticalID string, mutate func(metadata map[string]any))
	WorkflowPayloadFactory() *PipelinePayloadFactory
	OpcoSpinupRequestedPayload(ctx context.Context, verticalID string, approvalPayload map[string]any) map[string]any
}

type WorkflowModule interface {
	SemanticSource() semanticview.Source
	WorkflowDefinition() *WorkflowDefinition
	WorkflowNodes() []WorkflowNode
	GuardRegistry() GuardRegistry
	ActionRegistry() ActionRegistry
}

type workflowModuleScanPolicyProvider interface{ ScanPolicy() ScanPolicy }
type workflowModuleDiscoveryPolicyProvider interface{ DiscoveryPolicy() DiscoveryPolicy }
type workflowModuleScoringPolicyProvider interface{ ScoringPolicy() ScoringPolicy }
type workflowModulePayloadFactoryProvider interface{ PayloadFactory() PayloadFactory }

var defaultWorkflowModuleFactory func() WorkflowModule

func SetDefaultWorkflowModuleFactory(factory func() WorkflowModule) {
	defaultWorkflowModuleFactory = factory
}

func defaultWorkflowModule() WorkflowModule {
	module := defaultWorkflowModuleOrNil()
	if module == nil {
		panic("pipeline: workflow module is required; configure SetDefaultWorkflowModuleFactory or pass FactoryPipelineCoordinatorOptions.Module")
	}
	return module
}

func defaultWorkflowModuleOrNil() WorkflowModule {
	if defaultWorkflowModuleFactory == nil {
		return nil
	}
	return defaultWorkflowModuleFactory()
}

func DefaultWorkflowModuleOrNil() WorkflowModule {
	return defaultWorkflowModuleOrNil()
}

func DefaultWorkflowSemanticSourceOrNil() semanticview.Source {
	module := defaultWorkflowModuleOrNil()
	if module == nil {
		return nil
	}
	return module.SemanticSource()
}

func workflowModuleScanPolicy(module WorkflowModule) ScanPolicy {
	provider, ok := module.(workflowModuleScanPolicyProvider)
	if !ok {
		return nil
	}
	return provider.ScanPolicy()
}

func workflowModuleDiscoveryPolicy(module WorkflowModule) DiscoveryPolicy {
	provider, ok := module.(workflowModuleDiscoveryPolicyProvider)
	if !ok {
		return nil
	}
	return provider.DiscoveryPolicy()
}

func workflowModuleScoringPolicy(module WorkflowModule) ScoringPolicy {
	provider, ok := module.(workflowModuleScoringPolicyProvider)
	if !ok {
		return nil
	}
	return provider.ScoringPolicy()
}

func workflowModulePayloadFactory(module WorkflowModule) PayloadFactory {
	provider, ok := module.(workflowModulePayloadFactoryProvider)
	if !ok {
		return nil
	}
	return provider.PayloadFactory()
}
