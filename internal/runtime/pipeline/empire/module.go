package empire

import runtimepipeline "empireai/internal/runtime/pipeline"

type module struct{}

func NewModule() runtimepipeline.WorkflowModule {
	return module{}
}

func (module) DiscoveryPolicy() runtimepipeline.DiscoveryPolicy {
	return module{}
}

func (module) ScoringPolicy() runtimepipeline.ScoringPolicy {
	return module{}
}

func (module) PayloadFactory() runtimepipeline.PayloadFactory {
	return module{}
}

func (module) EvaluateDiscoveryPreFilter(payload map[string]any, rawSignal float64) (bool, float64, string) {
	return EvaluateDiscoveryPreFilter(payload, rawSignal)
}

func (module) BuildPrefilterSkipDetail(payload map[string]any, rawSignal, adjustedSignal float64, reason, mode string) map[string]any {
	return BuildPrefilterSkipDetail(payload, rawSignal, adjustedSignal, reason, mode)
}

func (module) NormalizeOpportunityPattern(raw string) string {
	return NormalizeOpportunityPattern(raw)
}

func (module) ExpectedScoringDimensions(rubric string) []string {
	return ExpectedScoringDimensions(rubric)
}

func (module) SelectScoringRubric(mode string) string {
	return SelectScoringRubric(mode)
}

func (module) ComputeComposite(in runtimepipeline.ScoringAccumulatorInput) runtimepipeline.ScoringComposite {
	return ComputeComposite(in)
}

func (module) BuildDiscoveryContextPayload(raw map[string]any) map[string]any {
	return BuildDiscoveryContextPayload(raw)
}

func (module) ResolveScoringAnalysisRecipient(recipients []string, excludedAgent string) string {
	return ResolveScoringAnalysisRecipient(recipients, excludedAgent)
}

func (module) NormalizeGeographicScope(raw string) string {
	return NormalizeGeographicScope(raw)
}

func (module) BuildScanAssignedPayload(scanID, campaignID, mode, geography string, source map[string]any, plannedShards int) runtimepipeline.ScanAssignedPayload {
	return BuildScanAssignedPayload(scanID, campaignID, mode, geography, source, plannedShards)
}

func (module) BuildSynthesisNeededPayload(scanID, campaignID, mode, geography string, raw map[string]any) runtimepipeline.SynthesisNeededPayload {
	return BuildSynthesisNeededPayload(scanID, campaignID, mode, geography, raw)
}

func (module) BuildDedupAmbiguousPayload(scanID, dedupEventID string, similarity float64, candidateName, geography string, signal float64, existingID, existingName string) runtimepipeline.DedupAmbiguousPayload {
	return BuildDedupAmbiguousPayload(scanID, dedupEventID, similarity, candidateName, geography, signal, existingID, existingName)
}

func (module) BuildVerticalDiscoveredPayload(verticalID, name, geography, mode, scanID, campaignID string, signal float64, discoverySource string, rawSignals map[string]any) runtimepipeline.VerticalDiscoveredPayload {
	return BuildVerticalDiscoveredPayload(verticalID, name, geography, mode, scanID, campaignID, signal, discoverySource, rawSignals)
}

func (module) BuildScanCompletedPayload(in runtimepipeline.ScanCompletedBuildInput) runtimepipeline.ScanCompletedPayload {
	return BuildScanCompletedPayload(in)
}

func (module) BuildScoringRequestedPayload(verticalID, verticalName, geography, mode, rubric string, dimensions []string, discoveryContext map[string]any) runtimepipeline.ScoringRequestedPayload {
	return BuildScoringRequestedPayload(verticalID, verticalName, geography, mode, rubric, dimensions, discoveryContext)
}

func (module) BuildScoringContestedPayload(verticalID, dimension string, contest runtimepipeline.ContestedDimension, rubric, mode string) runtimepipeline.ScoringContestedPayload {
	return BuildScoringContestedPayload(verticalID, dimension, contest, rubric, mode)
}

func (module) BuildVerticalScoredPayload(verticalID string, result runtimepipeline.ScoringComposite, verticalName, geography, mode string) runtimepipeline.VerticalScoredPayload {
	return BuildVerticalScoredPayload(verticalID, result, verticalName, geography, mode)
}

func (module) BuildVerticalShortlistedPayload(verticalID string, composite, viability float64, scoringPayload map[string]any) runtimepipeline.VerticalShortlistedPayload {
	return BuildVerticalShortlistedPayload(verticalID, composite, viability, scoringPayload)
}

func (module) BuildVerticalMarginalPayload(verticalID string, result runtimepipeline.ScoringComposite) runtimepipeline.VerticalMarginalPayload {
	return BuildVerticalMarginalPayload(verticalID, result)
}

func (module) BuildVerticalRejectedPayload(verticalID string, result runtimepipeline.ScoringComposite) runtimepipeline.VerticalRejectedPayload {
	return BuildVerticalRejectedPayload(verticalID, result)
}

func (module) BuildBrandRequestedPayload(verticalID, name, geography string, scoring, brief map[string]any) runtimepipeline.BrandRequestedPayload {
	return BuildBrandRequestedPayload(verticalID, name, geography, scoring, brief)
}

func (module) BuildValidationPackageReadyPayload(verticalID, name, geography string, snap runtimepipeline.ValidationContextSnapshot) runtimepipeline.ValidationPackageReadyPayload {
	return BuildValidationPackageReadyPayload(verticalID, name, geography, snap)
}

func (module) BuildSpecValidationRequestedPayload(verticalID string, spec map[string]any) runtimepipeline.SpecValidationRequestedPayload {
	return BuildSpecValidationRequestedPayload(verticalID, spec)
}

func (module) BuildCTOSpecReviewRequestedPayload(verticalID, name, geography string, specValidation map[string]any, snap runtimepipeline.ValidationContextSnapshot) runtimepipeline.CTOSpecReviewRequestedPayload {
	return BuildCTOSpecReviewRequestedPayload(verticalID, name, geography, specValidation, snap)
}

func (module) BuildSpecRevisionRequestedPayload(verticalID, source, name, geography string, feedback map[string]any, snap runtimepipeline.ValidationContextSnapshot) runtimepipeline.SpecRevisionRequestedPayload {
	return BuildSpecRevisionRequestedPayload(verticalID, source, name, geography, feedback, snap)
}

func (module) BuildValidationMoreDataPayload(verticalID, name, geography string, request map[string]any, snap runtimepipeline.ValidationContextSnapshot) runtimepipeline.ValidationMoreDataNeededPayload {
	return BuildValidationMoreDataPayload(verticalID, name, geography, request, snap)
}

func (module) BuildBrandRevisionNeededPayload(verticalID, name, geography string, feedback, brand map[string]any) runtimepipeline.BrandRevisionNeededPayload {
	return BuildBrandRevisionNeededPayload(verticalID, name, geography, feedback, brand)
}

func (module) BuildVerticalKilledPayload(verticalID, name, geography, sourceEvent string, reason map[string]any) runtimepipeline.VerticalKilledPayload {
	return BuildVerticalKilledPayload(verticalID, name, geography, sourceEvent, reason)
}

func (module) BuildValidationStartedPayload(verticalID, name, geography string, scoring map[string]any) runtimepipeline.ValidationStartedPayload {
	return BuildValidationStartedPayload(verticalID, name, geography, scoring)
}
