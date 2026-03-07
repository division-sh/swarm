package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

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

func ExpectedScoringDimensionsForTest(rubric string) []string {
	return expectedScoringDimensions(rubric)
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
		} else {
			pc.notifyTestVerticalStageUpdated(verticalID, stage)
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
