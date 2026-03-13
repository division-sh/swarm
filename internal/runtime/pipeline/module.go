package pipeline

import (
	"context"

	empirepayloads "empireai/internal/empire/payloads"
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

type ValidationStartedPayload = empirepayloads.ValidationStartedPayload
type BrandRequestedPayload = empirepayloads.BrandRequestedPayload
type ValidationPackageReadyPayload = empirepayloads.ValidationPackageReadyPayload
type SpecValidationRequestedPayload = empirepayloads.SpecValidationRequestedPayload
type CTOSpecReviewRequestedPayload = empirepayloads.CTOSpecReviewRequestedPayload
type SpecRevisionRequestedPayload = empirepayloads.SpecRevisionRequestedPayload
type ValidationMoreDataNeededPayload = empirepayloads.ValidationMoreDataNeededPayload
type BrandRevisionNeededPayload = empirepayloads.BrandRevisionNeededPayload
type VerticalKilledPayload = empirepayloads.VerticalKilledPayload
type ScanAssignedPayload = empirepayloads.ScanAssignedPayload
type SynthesisNeededPayload = empirepayloads.SynthesisNeededPayload
type DedupAmbiguousPayload = empirepayloads.DedupAmbiguousPayload
type VerticalDiscoveredPayload = empirepayloads.VerticalDiscoveredPayload
type ScanCompletedBuildInput = empirepayloads.ScanCompletedBuildInput
type ScanCompletedPayload = empirepayloads.ScanCompletedPayload
type ScoringRequestedPayload = empirepayloads.ScoringRequestedPayload
type ScoringContestedPayload = empirepayloads.ScoringContestedPayload
type ValidationContextSnapshot = empirepayloads.ValidationContextSnapshot
type ScoreDimensionResult = empirepayloads.ScoreDimensionResult
type ContestedDimension = empirepayloads.ContestedDimension
type ScoringComposite = empirepayloads.ScoringComposite
type ScoringAccumulatorInput = empirepayloads.ScoringAccumulatorInput
type VerticalScoredPayload = empirepayloads.VerticalScoredPayload
type VerticalShortlistedPayload = empirepayloads.VerticalShortlistedPayload
type VerticalMarginalPayload = empirepayloads.VerticalMarginalPayload
type VerticalRejectedPayload = empirepayloads.VerticalRejectedPayload
type PortfolioDigestTimerPayload = empirepayloads.PortfolioDigestTimerPayload

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
	ExpandScanAssignments(mode string, payload map[string]any, assigned ScanAssignedPayload, batchSize int) ([]ScanAssignedPayload, error)
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

type WorkflowHookContext struct {
	Event      events.Event
	VerticalID string
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
