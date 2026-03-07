package pipeline

import (
	"context"
	"log"
	"strings"

	empirepipeline "empireai/internal/runtime/pipeline/empire"
)

type PipelinePayloadFactory struct {
	coordinator *FactoryPipelineCoordinator
}

func NewPipelinePayloadFactory(coordinator *FactoryPipelineCoordinator) *PipelinePayloadFactory {
	return &PipelinePayloadFactory{coordinator: coordinator}
}

func (pf *PipelinePayloadFactory) ValidationContext(verticalID string) validationContextSnapshot {
	if pf == nil || pf.coordinator == nil || strings.TrimSpace(verticalID) == "" {
		return validationContextSnapshot{
			Research: map[string]any{},
			Spec:     map[string]any{},
			CTONotes: map[string]any{},
			Brand:    map[string]any{},
			Scoring:  map[string]any{},
		}
	}
	pf.coordinator.mu.Lock()
	defer pf.coordinator.mu.Unlock()
	st := pf.coordinator.validationGate.getStateLocked(verticalID)
	return validationContextSnapshot{
		Research:    parsePayloadMap(st.ResearchPayload),
		Spec:        parsePayloadMap(st.SpecPayload),
		CTONotes:    parsePayloadMap(st.CTOPayload),
		Brand:       parsePayloadMap(st.BrandPayload),
		Scoring:     parsePayloadMap(st.ScoringPayload),
		SpecVersion: st.SpecVersion,
	}
}

func (pf *PipelinePayloadFactory) identityForPayload(ctx context.Context, verticalID string) (string, string) {
	name, geography, err := pf.coordinator.loadVerticalIdentity(ctx, verticalID)
	if err != nil {
		log.Printf("pipeline: identity lookup failed vertical=%s err=%v", verticalID, err)
		return "", ""
	}
	return strings.TrimSpace(name), strings.TrimSpace(geography)
}

func (pf *PipelinePayloadFactory) BuildScanAssignedPayload(
	scanID, campaignID, mode, geography string,
	source map[string]any,
	plannedShards int,
) ScanAssignedPayload {
	return empirepipeline.BuildScanAssignedPayload(scanID, campaignID, mode, geography, source, plannedShards)
}

func (pf *PipelinePayloadFactory) BuildSynthesisNeededPayload(scanID string, acc *scanAccumulator, raw map[string]any) SynthesisNeededPayload {
	campaignID, mode, geography := "", "", ""
	if acc != nil {
		campaignID = acc.CampaignID
		mode = acc.Mode
		geography = acc.Geography
	}
	out := empirepipeline.BuildSynthesisNeededPayload(scanID, campaignID, mode, geography, raw)
	if acc != nil {
		out.Geography = firstNonEmptyString(strings.TrimSpace(out.Geography), strings.TrimSpace(acc.Geography))
	}
	return out
}

func (pf *PipelinePayloadFactory) BuildDedupAmbiguousPayload(
	scanID, dedupEventID string,
	similarity float64,
	candidateName, geography string,
	signal float64,
	existingID, existingName string,
) DedupAmbiguousPayload {
	return empirepipeline.BuildDedupAmbiguousPayload(scanID, dedupEventID, similarity, candidateName, geography, signal, existingID, existingName)
}

func (pf *PipelinePayloadFactory) BuildVerticalDiscoveredPayload(
	verticalID, name, geography, mode, scanID, campaignID string,
	signal float64,
	discoverySource string,
	rawSignals map[string]any,
) VerticalDiscoveredPayload {
	return empirepipeline.BuildVerticalDiscoveredPayload(verticalID, name, geography, mode, scanID, campaignID, signal, discoverySource, rawSignals)
}

func (pf *PipelinePayloadFactory) BuildScanCompletedPayload(in scanCompletedBuildInput) ScanCompletedPayload {
	return empirepipeline.BuildScanCompletedPayload(in)
}

func (pf *PipelinePayloadFactory) BuildScoringRequestedPayload(verticalID string, acc *scoringAccumulator) ScoringRequestedPayload {
	if acc == nil {
		return empirepipeline.BuildScoringRequestedPayload(strings.TrimSpace(verticalID), "", "", "", "", nil, nil)
	}
	return empirepipeline.BuildScoringRequestedPayload(
		strings.TrimSpace(verticalID),
		acc.VerticalName,
		acc.Geography,
		acc.Mode,
		acc.Rubric,
		acc.Expected,
		acc.DiscoveryContext,
	)
}

func (pf *PipelinePayloadFactory) DerivedScoringGeneratorAgent(ctx context.Context, acc *scoringAccumulator) string {
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
	if pf == nil || pf.coordinator == nil || pf.coordinator.db == nil {
		return raw
	}

	var agentID string
	_ = dbQueryRowContext(ctx, pf.coordinator.db, `
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

func (pf *PipelinePayloadFactory) SelectScoringAnalysisRecipient(excludedAgent string) string {
	if pf == nil || pf.coordinator == nil || pf.coordinator.bus == nil {
		return ""
	}
	return empirepipeline.ResolveScoringAnalysisRecipient(
		uniqueStrings(pf.coordinator.bus.ResolveSubscribedRecipients("scoring.requested")),
		excludedAgent,
	)
}

func (pf *PipelinePayloadFactory) BuildScoringContestedPayload(verticalID, dimension string, contest contestedDimension, acc *scoringAccumulator) ScoringContestedPayload {
	rubric, mode := "", ""
	if acc != nil {
		rubric = acc.Rubric
		mode = acc.Mode
	}
	return empirepipeline.BuildScoringContestedPayload(verticalID, dimension, contest, rubric, mode)
}

func (pf *PipelinePayloadFactory) BuildVerticalScoredPayload(verticalID string, result scoringComposite, acc *scoringAccumulator) VerticalScoredPayload {
	verticalName, geography, mode := "", "", ""
	if acc != nil {
		verticalName = acc.VerticalName
		geography = acc.Geography
		mode = acc.Mode
	}
	return empirepipeline.BuildVerticalScoredPayload(verticalID, result, verticalName, geography, mode)
}

func (pf *PipelinePayloadFactory) BuildVerticalShortlistedPayload(verticalID string, composite, viability float64, scoringPayload map[string]any) VerticalShortlistedPayload {
	return empirepipeline.BuildVerticalShortlistedPayload(verticalID, composite, viability, scoringPayload)
}

func (pf *PipelinePayloadFactory) BuildVerticalMarginalPayload(verticalID string, result scoringComposite) VerticalMarginalPayload {
	return empirepipeline.BuildVerticalMarginalPayload(verticalID, result)
}

func (pf *PipelinePayloadFactory) BuildVerticalRejectedPayload(verticalID string, result scoringComposite) VerticalRejectedPayload {
	return empirepipeline.BuildVerticalRejectedPayload(verticalID, result)
}

func (pf *PipelinePayloadFactory) BuildBrandRequestedPayload(ctx context.Context, verticalID string, scoring map[string]any, brief map[string]any) BrandRequestedPayload {
	name, geography := pf.identityForPayload(ctx, verticalID)
	return empirepipeline.BuildBrandRequestedPayload(verticalID, name, geography, scoring, brief)
}

func (pf *PipelinePayloadFactory) BuildValidationPackageReadyPayload(ctx context.Context, verticalID string, snap validationContextSnapshot) ValidationPackageReadyPayload {
	name, geography := pf.identityForPayload(ctx, verticalID)
	return empirepipeline.BuildValidationPackageReadyPayload(verticalID, name, geography, snap)
}

func (pf *PipelinePayloadFactory) BuildSpecValidationRequestedPayload(ctx context.Context, verticalID string, spec map[string]any) SpecValidationRequestedPayload {
	return empirepipeline.BuildSpecValidationRequestedPayload(verticalID, spec)
}

func (pf *PipelinePayloadFactory) BuildCTOSpecReviewRequestedPayload(ctx context.Context, verticalID string, specValidation map[string]any) CTOSpecReviewRequestedPayload {
	snap := pf.ValidationContext(verticalID)
	name, geography := pf.identityForPayload(ctx, verticalID)
	return empirepipeline.BuildCTOSpecReviewRequestedPayload(verticalID, name, geography, specValidation, snap)
}

func (pf *PipelinePayloadFactory) BuildSpecRevisionRequestedPayload(ctx context.Context, verticalID, source string, feedback map[string]any) SpecRevisionRequestedPayload {
	snap := pf.ValidationContext(verticalID)
	name, geography := pf.identityForPayload(ctx, verticalID)
	return empirepipeline.BuildSpecRevisionRequestedPayload(verticalID, source, name, geography, feedback, snap)
}

func (pf *PipelinePayloadFactory) BuildValidationMoreDataPayload(ctx context.Context, verticalID string, request map[string]any, snap validationContextSnapshot) ValidationMoreDataNeededPayload {
	name, geography := pf.identityForPayload(ctx, verticalID)
	return empirepipeline.BuildValidationMoreDataPayload(verticalID, name, geography, request, snap)
}

func (pf *PipelinePayloadFactory) BuildBrandRevisionNeededPayload(ctx context.Context, verticalID string, feedback map[string]any, brand map[string]any) BrandRevisionNeededPayload {
	name, geography := pf.identityForPayload(ctx, verticalID)
	return empirepipeline.BuildBrandRevisionNeededPayload(verticalID, name, geography, feedback, brand)
}

func (pf *PipelinePayloadFactory) BuildVerticalKilledPayload(ctx context.Context, verticalID, sourceEvent string, reason map[string]any) VerticalKilledPayload {
	name, geography := pf.identityForPayload(ctx, verticalID)
	return empirepipeline.BuildVerticalKilledPayload(verticalID, name, geography, sourceEvent, reason)
}

func (pf *PipelinePayloadFactory) BuildValidationStartedPayload(ctx context.Context, verticalID string, scoring map[string]any, seed map[string]any) ValidationStartedPayload {
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
	dbName, dbGeography, err := pf.coordinator.loadVerticalIdentity(ctx, verticalID)
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
	return empirepipeline.BuildValidationStartedPayload(verticalID, name, geography, scoring)
}

func buildDiscoveryContextPayload(raw map[string]any) map[string]any {
	return empirepipeline.BuildDiscoveryContextPayload(raw)
}

func normalizeOpportunityPattern(raw string) string {
	return empirepipeline.NormalizeOpportunityPattern(raw)
}

func normalizeGeographicScope(raw string) string {
	return empirepipeline.NormalizeGeographicScope(raw)
}
