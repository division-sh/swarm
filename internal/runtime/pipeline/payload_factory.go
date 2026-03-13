package pipeline

import (
	"context"
	"log"
	"strings"
)

type PipelinePayloadFactory struct {
	module      PayloadFactory
	scoring     ScoringPolicy
	coordinator *FactoryPipelineCoordinator
}

func NewPipelinePayloadFactory(module PayloadFactory, scoring ScoringPolicy, coordinator *FactoryPipelineCoordinator) *PipelinePayloadFactory {
	return &PipelinePayloadFactory{module: module, scoring: scoring, coordinator: coordinator}
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
	st := pf.coordinator.validationStateSnapshot(verticalID)
	if st == nil {
		return pf.validationContextFromWorkflowProjection(verticalID, validationContextSnapshot{
			Research: map[string]any{},
			Spec:     map[string]any{},
			CTONotes: map[string]any{},
			Brand:    map[string]any{},
			Scoring:  map[string]any{},
		})
	}
	return pf.validationContextFromWorkflowProjection(verticalID, validationContextSnapshot{
		Research:    parsePayloadMap(st.ResearchPayload),
		Spec:        parsePayloadMap(st.SpecPayload),
		CTONotes:    parsePayloadMap(st.CTOPayload),
		Brand:       parsePayloadMap(st.BrandPayload),
		Scoring:     parsePayloadMap(st.ScoringPayload),
		SpecVersion: st.SpecVersion,
	})
}

func (pf *PipelinePayloadFactory) validationContextFromWorkflowProjection(verticalID string, snap validationContextSnapshot) validationContextSnapshot {
	if pf == nil || pf.coordinator == nil || pf.coordinator.workflowStore == nil || !pf.coordinator.workflowStore.Enabled() {
		return snap
	}
	instance, ok, err := pf.coordinator.workflowStore.Load(context.Background(), verticalID)
	if err != nil || !ok {
		return snap
	}
	entityProjection, _ := workflowEntityProjectionBucket(instance)
	if len(entityProjection) == 0 {
		return snap
	}
	if len(snap.Research) == 0 {
		if brief, ok := asObject(entityProjection["business_brief"]); ok && len(brief) > 0 {
			snap.Research = cloneStringAnyMap(brief)
		} else if researchContext, ok := asObject(entityProjection["research_context"]); ok && len(researchContext) > 0 {
			snap.Research = cloneStringAnyMap(researchContext)
		}
	}
	if len(snap.Spec) == 0 {
		if spec, ok := asObject(entityProjection["mvp_spec"]); ok && len(spec) > 0 {
			snap.Spec = cloneStringAnyMap(spec)
		}
	}
	if len(snap.CTONotes) == 0 {
		if ctoNotes, ok := asObject(entityProjection["cto_feasibility"]); ok && len(ctoNotes) > 0 {
			snap.CTONotes = cloneStringAnyMap(ctoNotes)
		}
	}
	if len(snap.Brand) == 0 {
		if brand, ok := asObject(entityProjection["brand"]); ok && len(brand) > 0 {
			snap.Brand = cloneStringAnyMap(brand)
		}
	}
	return snap
}

func (pf *PipelinePayloadFactory) identityForPayload(ctx context.Context, verticalID string) (string, string) {
	name, geography, err := pf.coordinator.loadEntityIdentity(ctx, verticalID)
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
) map[string]any {
	return pf.module.BuildScanAssignedPayload(scanID, campaignID, mode, geography, source, plannedShards)
}

func (pf *PipelinePayloadFactory) BuildSynthesisNeededPayload(scanID string, acc *scanAccumulator, raw map[string]any) map[string]any {
	campaignID, mode, geography := "", "", ""
	if acc != nil {
		campaignID = acc.CampaignID
		mode = acc.Mode
		geography = acc.Geography
	}
	out := pf.module.BuildSynthesisNeededPayload(scanID, campaignID, mode, geography, raw)
	if acc != nil {
		out["geography"] = firstNonEmptyString(strings.TrimSpace(asString(out["geography"])), strings.TrimSpace(acc.Geography))
	}
	return out
}

func (pf *PipelinePayloadFactory) BuildDedupAmbiguousPayload(
	scanID, dedupEventID string,
	similarity float64,
	candidateName, geography string,
	signal float64,
	existingID, existingName string,
) map[string]any {
	return pf.module.BuildDedupAmbiguousPayload(scanID, dedupEventID, similarity, candidateName, geography, signal, existingID, existingName)
}

func (pf *PipelinePayloadFactory) BuildVerticalDiscoveredPayload(
	verticalID, name, geography, mode, scanID, campaignID string,
	signal float64,
	discoverySource string,
	rawSignals map[string]any,
) map[string]any {
	return pf.module.BuildVerticalDiscoveredPayload(verticalID, name, geography, mode, scanID, campaignID, signal, discoverySource, rawSignals)
}

func (pf *PipelinePayloadFactory) BuildScanCompletedPayload(in scanCompletedBuildInput) map[string]any {
	return pf.module.BuildScanCompletedPayload(in)
}

func (pf *PipelinePayloadFactory) BuildScoringRequestedPayload(verticalID string, acc *scoringAccumulator) map[string]any {
	if acc == nil {
		return pf.module.BuildScoringRequestedPayload(strings.TrimSpace(verticalID), "", "", "", "", nil, nil)
	}
	return pf.module.BuildScoringRequestedPayload(
		strings.TrimSpace(verticalID),
		acc.EntityName,
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
	return pf.scoring.ResolveScoringAnalysisRecipient(
		uniqueStrings(pf.coordinator.bus.ResolveSubscribedRecipients("scoring.requested")),
		excludedAgent,
	)
}

func (pf *PipelinePayloadFactory) BuildScoringContestedPayload(verticalID, dimension string, contest contestedDimension, acc *scoringAccumulator) map[string]any {
	rubric, mode := "", ""
	if acc != nil {
		rubric = acc.Rubric
		mode = acc.Mode
	}
	return pf.module.BuildScoringContestedPayload(verticalID, dimension, contest, rubric, mode)
}

func (pf *PipelinePayloadFactory) BuildVerticalScoredPayload(verticalID string, result scoringComposite, acc *scoringAccumulator) map[string]any {
	verticalName, geography, mode := "", "", ""
	if acc != nil {
		verticalName = acc.EntityName
		geography = acc.Geography
		mode = acc.Mode
	}
	return pf.module.BuildVerticalScoredPayload(verticalID, result, verticalName, geography, mode)
}

func (pf *PipelinePayloadFactory) BuildVerticalShortlistedPayload(verticalID string, composite, viability float64, scoringPayload map[string]any) map[string]any {
	return pf.module.BuildVerticalShortlistedPayload(verticalID, composite, viability, scoringPayload)
}

func (pf *PipelinePayloadFactory) BuildVerticalMarginalPayload(verticalID string, result scoringComposite) map[string]any {
	return pf.module.BuildVerticalMarginalPayload(verticalID, result)
}

func (pf *PipelinePayloadFactory) BuildVerticalRejectedPayload(verticalID string, result scoringComposite) map[string]any {
	return pf.module.BuildVerticalRejectedPayload(verticalID, result)
}

func (pf *PipelinePayloadFactory) BuildBrandRequestedPayload(ctx context.Context, verticalID string, scoring map[string]any, brief map[string]any) map[string]any {
	name, geography := pf.identityForPayload(ctx, verticalID)
	return pf.module.BuildBrandRequestedPayload(verticalID, name, geography, scoring, brief)
}

func (pf *PipelinePayloadFactory) BuildValidationPackageReadyPayload(ctx context.Context, verticalID string, snap validationContextSnapshot) map[string]any {
	name, geography := pf.identityForPayload(ctx, verticalID)
	return pf.module.BuildValidationPackageReadyPayload(verticalID, name, geography, snap)
}

func (pf *PipelinePayloadFactory) BuildSpecValidationRequestedPayload(ctx context.Context, verticalID string, spec map[string]any) map[string]any {
	return pf.module.BuildSpecValidationRequestedPayload(verticalID, spec)
}

func (pf *PipelinePayloadFactory) BuildCTOSpecReviewRequestedPayload(ctx context.Context, verticalID string, specValidation map[string]any) map[string]any {
	snap := pf.ValidationContext(verticalID)
	name, geography := pf.identityForPayload(ctx, verticalID)
	return pf.module.BuildCTOSpecReviewRequestedPayload(verticalID, name, geography, specValidation, snap)
}

func (pf *PipelinePayloadFactory) BuildSpecRevisionRequestedPayload(ctx context.Context, verticalID, source string, feedback map[string]any) map[string]any {
	snap := pf.ValidationContext(verticalID)
	name, geography := pf.identityForPayload(ctx, verticalID)
	return pf.module.BuildSpecRevisionRequestedPayload(verticalID, source, name, geography, feedback, snap)
}

func (pf *PipelinePayloadFactory) BuildValidationMoreDataPayload(ctx context.Context, verticalID string, request map[string]any, snap validationContextSnapshot) map[string]any {
	name, geography := pf.identityForPayload(ctx, verticalID)
	return pf.module.BuildValidationMoreDataPayload(verticalID, name, geography, request, snap)
}

func (pf *PipelinePayloadFactory) BuildBrandRevisionNeededPayload(ctx context.Context, verticalID string, feedback map[string]any, brand map[string]any) map[string]any {
	name, geography := pf.identityForPayload(ctx, verticalID)
	return pf.module.BuildBrandRevisionNeededPayload(verticalID, name, geography, feedback, brand)
}

func (pf *PipelinePayloadFactory) BuildVerticalKilledPayload(ctx context.Context, verticalID, sourceEvent string, reason map[string]any) map[string]any {
	name, geography := pf.identityForPayload(ctx, verticalID)
	return pf.module.BuildVerticalKilledPayload(verticalID, name, geography, sourceEvent, reason)
}

func (pf *PipelinePayloadFactory) BuildValidationStartedPayload(ctx context.Context, verticalID string, scoring map[string]any, seed map[string]any) map[string]any {
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
	dbName, dbGeography, err := pf.coordinator.loadEntityIdentity(ctx, verticalID)
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
	return pf.module.BuildValidationStartedPayload(verticalID, name, geography, scoring)
}
