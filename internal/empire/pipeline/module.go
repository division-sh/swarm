package empire

import (
	"path/filepath"
	"sync"

	runtimecontracts "empireai/internal/runtime/contracts"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/semanticview"
)

type module struct {
	once           *sync.Once
	contractBundle *runtimecontracts.WorkflowContractBundle
	workflow       *runtimepipeline.WorkflowDefinition
	workflowNodes  []runtimepipeline.WorkflowNode
	guardRegistry  runtimepipeline.GuardRegistry
	actionRegistry runtimepipeline.ActionRegistry
	loadErr        error
}

func NewModule() runtimepipeline.WorkflowModule {
	return &module{}
}

func (m *module) init() {
	if m.once == nil {
		m.once = &sync.Once{}
	}
	m.once.Do(func() {
		repoRoot := runtimepipeline.WorkflowRepoRoot()
		m.contractBundle, m.loadErr = runtimecontracts.LoadWorkflowContractBundleWithOverrides(
			repoRoot,
			filepath.Join(repoRoot, "docs", "specs", "mas-platform", "empire", "contracts"),
			filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml"),
		)
		if m.loadErr != nil {
			return
		}
		m.workflow, m.loadErr = runtimepipeline.LoadWorkflowDefinition(semanticview.Wrap(m.contractBundle))
		if m.loadErr != nil {
			return
		}
		m.workflowNodes, m.loadErr = runtimepipeline.LoadWorkflowNodes(semanticview.Wrap(m.contractBundle))
		if m.loadErr != nil {
			return
		}
		source := semanticview.Wrap(m.contractBundle)
		m.guardRegistry = runtimepipeline.NewContractGuardRegistry(source)
		m.actionRegistry = runtimepipeline.NewContractActionRegistry(source)
	})
	if m.loadErr != nil {
		panic(m.loadErr)
	}
}

func (m *module) SemanticSource() semanticview.Source {
	m.init()
	return semanticview.Wrap(m.contractBundle)
}

func (m *module) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	m.init()
	return m.workflow
}

func (m *module) WorkflowNodes() []runtimepipeline.WorkflowNode {
	m.init()
	out := make([]runtimepipeline.WorkflowNode, 0, len(m.workflowNodes))
	for _, node := range m.workflowNodes {
		nodeCopy := node
		out = append(out, nodeCopy)
	}
	return out
}

func (m *module) GuardRegistry() runtimepipeline.GuardRegistry {
	m.init()
	return m.guardRegistry
}

func (m *module) ActionRegistry() runtimepipeline.ActionRegistry {
	m.init()
	return m.actionRegistry
}

func (*module) DiscoveryPolicy() runtimepipeline.DiscoveryPolicy {
	return module{}
}

func (*module) ScanPolicy() runtimepipeline.ScanPolicy {
	return module{}
}

func (*module) ScoringPolicy() runtimepipeline.ScoringPolicy {
	return module{}
}

func (*module) PayloadFactory() runtimepipeline.PayloadFactory {
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

func (module) BuildDiscoveryCandidatesForReport(scanMode string, payload map[string]any) []runtimepipeline.DiscoveryCandidate {
	return BuildDiscoveryCandidatesForReport(scanMode, payload)
}

func (module) ExpectedScoringDimensions(rubric string) []string {
	return ExpectedScoringDimensions(rubric)
}

func (module) SelectScoringRubric(mode string) string {
	return SelectScoringRubric(mode)
}

func (module) ComputeComposite(in runtimepipeline.ScoringAccumulatorInput) runtimepipeline.ScoringComposite {
	result := ComputeComposite(runtimeScoringAccumulatorInput(in))
	return runtimepipeline.ScoringComposite{
		Result:         result.Result,
		Reason:         result.Reason,
		CompositeScore: result.CompositeScore,
		ViabilityScore: result.ViabilityScore,
		MarketScore:    result.MarketScore,
		Dimensions:     runtimepipelineScoreDimensionResultMap(result.Dimensions),
		Rubric:         result.Rubric,
		Partial:        result.Partial,
	}
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

func (module) ScoringRestoreDeltaBucket() string {
	return "scoring-restore"
}

func (module) EncodeScoringRestoreDelta(acc *runtimepipeline.ScoringAccumulator) map[string]any {
	return runtimepipeline.EncodeScoringRestoreDelta(acc)
}

func (module) ApplyScoringRestoreDelta(acc *runtimepipeline.ScoringAccumulator, delta map[string]any) {
	runtimepipeline.ApplyScoringRestoreDelta(acc, delta)
}

func (module) BuildScanAssignedPayload(scanID, campaignID, mode, geography string, source map[string]any, plannedShards int) map[string]any {
	return payloadMap(BuildScanAssignedPayload(scanID, campaignID, mode, geography, source, plannedShards))
}

func (module) BuildSynthesisNeededPayload(scanID, campaignID, mode, geography string, raw map[string]any) map[string]any {
	return payloadMap(BuildSynthesisNeededPayload(scanID, campaignID, mode, geography, raw))
}

func (module) BuildDedupAmbiguousPayload(scanID, dedupEventID string, similarity float64, candidateName, geography string, signal float64, existingID, existingName string) map[string]any {
	return payloadMap(BuildDedupAmbiguousPayload(scanID, dedupEventID, similarity, candidateName, geography, signal, existingID, existingName))
}

func (module) BuildVerticalDiscoveredPayload(verticalID, name, geography, mode, scanID, campaignID string, signal float64, discoverySource string, rawSignals map[string]any) map[string]any {
	return payloadMap(BuildVerticalDiscoveredPayload(verticalID, name, geography, mode, scanID, campaignID, signal, discoverySource, rawSignals))
}

func (module) BuildScanCompletedPayload(in runtimepipeline.ScanCompletedBuildInput) map[string]any {
	return payloadMap(BuildScanCompletedPayload(runtimeScanCompletedBuildInput(in)))
}

func (module) BuildScoringRequestedPayload(verticalID, verticalName, geography, mode, rubric string, dimensions []string, discoveryContext map[string]any) map[string]any {
	return payloadMap(BuildScoringRequestedPayload(verticalID, verticalName, geography, mode, rubric, dimensions, discoveryContext))
}

func (module) BuildScoringContestedPayload(verticalID, dimension string, contest runtimepipeline.ContestedDimension, rubric, mode string) map[string]any {
	return payloadMap(BuildScoringContestedPayload(verticalID, dimension, runtimeContestedDimension(contest), rubric, mode))
}

func (module) BuildVerticalScoredPayload(verticalID string, result runtimepipeline.ScoringComposite, verticalName, geography, mode string) map[string]any {
	return payloadMap(BuildVerticalScoredPayload(verticalID, runtimeScoringComposite(result), verticalName, geography, mode))
}

func (module) BuildVerticalShortlistedPayload(verticalID string, composite, viability float64, scoringPayload map[string]any) map[string]any {
	return payloadMap(BuildVerticalShortlistedPayload(verticalID, composite, viability, scoringPayload))
}

func (module) BuildVerticalMarginalPayload(verticalID string, result runtimepipeline.ScoringComposite) map[string]any {
	return payloadMap(BuildVerticalMarginalPayload(verticalID, runtimeScoringComposite(result)))
}

func (module) BuildVerticalRejectedPayload(verticalID string, result runtimepipeline.ScoringComposite) map[string]any {
	return payloadMap(BuildVerticalRejectedPayload(verticalID, runtimeScoringComposite(result)))
}

func (module) BuildBrandRequestedPayload(verticalID, name, geography string, scoring, brief map[string]any) map[string]any {
	return payloadMap(BuildBrandRequestedPayload(verticalID, name, geography, scoring, brief))
}

func (module) BuildValidationPackageReadyPayload(verticalID, name, geography string, snap runtimepipeline.ValidationContextSnapshot) map[string]any {
	return payloadMap(BuildValidationPackageReadyPayload(verticalID, name, geography, runtimeValidationSnapshot(snap)))
}

func (module) BuildSpecValidationRequestedPayload(verticalID string, spec map[string]any) map[string]any {
	return payloadMap(BuildSpecValidationRequestedPayload(verticalID, spec))
}

func (module) BuildCTOSpecReviewRequestedPayload(verticalID, name, geography string, specValidation map[string]any, snap runtimepipeline.ValidationContextSnapshot) map[string]any {
	return payloadMap(BuildCTOSpecReviewRequestedPayload(verticalID, name, geography, specValidation, runtimeValidationSnapshot(snap)))
}

func (module) BuildSpecRevisionRequestedPayload(verticalID, source, name, geography string, feedback map[string]any, snap runtimepipeline.ValidationContextSnapshot) map[string]any {
	return payloadMap(BuildSpecRevisionRequestedPayload(verticalID, source, name, geography, feedback, runtimeValidationSnapshot(snap)))
}

func (module) BuildValidationMoreDataPayload(verticalID, name, geography string, request map[string]any, snap runtimepipeline.ValidationContextSnapshot) map[string]any {
	return payloadMap(BuildValidationMoreDataPayload(verticalID, name, geography, request, runtimeValidationSnapshot(snap)))
}

func (module) BuildBrandRevisionNeededPayload(verticalID, name, geography string, feedback, brand map[string]any) map[string]any {
	return payloadMap(BuildBrandRevisionNeededPayload(verticalID, name, geography, feedback, brand))
}

func (module) BuildVerticalKilledPayload(verticalID, name, geography, sourceEvent string, reason map[string]any) map[string]any {
	return payloadMap(BuildVerticalKilledPayload(verticalID, name, geography, sourceEvent, reason))
}

func (module) BuildValidationStartedPayload(verticalID, name, geography string, scoring map[string]any) map[string]any {
	return payloadMap(BuildValidationStartedPayload(verticalID, name, geography, scoring))
}
