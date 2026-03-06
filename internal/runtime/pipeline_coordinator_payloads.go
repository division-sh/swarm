package runtime

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"strings"
	"time"
)

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
		DedupID:      strings.TrimSpace(dedupEventID),
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

func firstNonEmptyMap(maps ...map[string]any) map[string]any {
	for _, item := range maps {
		if len(item) > 0 {
			return item
		}
	}
	return map[string]any{}
}

func summarizeContractPayload(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(t)
	case map[string]any:
		if text := strings.TrimSpace(asString(t["questions"])); text != "" {
			return text
		}
		if text := strings.TrimSpace(asString(t["cto_feedback"])); text != "" {
			return text
		}
		if text := strings.TrimSpace(asString(t["feedback"])); text != "" {
			return text
		}
		if b, err := json.Marshal(t); err == nil {
			return strings.TrimSpace(string(b))
		}
	default:
		if b, err := json.Marshal(t); err == nil {
			return strings.TrimSpace(string(b))
		}
	}
	return strings.TrimSpace(asString(v))
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
	businessBrief := parsePayloadMap(nil)
	if snap.Research != nil {
		if brief, ok := snap.Research["business_brief"].(map[string]any); ok && brief != nil {
			businessBrief = brief
		} else {
			businessBrief = snap.Research
		}
	}
	return CTOSpecReviewRequestedPayload{
		VerticalID:    strings.TrimSpace(verticalID),
		MvPSpec:       summarizeContractPayload(firstNonEmptyMap(specValidation, snap.Spec)),
		BusinessBrief: businessBrief,
		VerticalContext: map[string]any{
			"vertical_name": name,
			"geography":     geography,
			"scoring":       snap.Scoring,
		},
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
		CTOFeedback:  summarizeContractPayload(feedback),
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
		Questions:    summarizeContractPayload(request),
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
	out.ScoringContext = summarizeContractPayload(scoring)
	return out
}
