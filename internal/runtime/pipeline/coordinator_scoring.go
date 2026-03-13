package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

const ScoringNodeID = "scoring-node"

type scoringBackgroundRuntime interface {
	handleScoringRequested(context.Context, events.Event)
	handleVerticalDerived(context.Context, events.Event)
	handleScoreDimensionComplete(context.Context, events.Event)
	handleScoringContestResolved(context.Context, events.Event)
}

type scoringStateRuntime interface {
	scoringBackgroundRuntime
	loadScoringSeed(context.Context, string) (string, string, string)
	loadWorkflowScoringAccumulator(context.Context, string) (*scoringAccumulator, bool)
	publish(context.Context, string, string, map[string]any)
	applyWorkflowEventTransition(context.Context, events.Event) (workflowTransitionOutcome, bool)
	appendScoringDigestBuffer(context.Context, map[string]any)
	persistWorkflowScoringAccumulator(context.Context, *scoringAccumulator)
	clearWorkflowScoringAccumulator(context.Context, string)
}

func ExpectedScoringDimensionsForTest(rubric string) []string {
	module := defaultWorkflowModule()
	policy := workflowModuleScoringPolicy(module)
	if policy == nil {
		return nil
	}
	return policy.ExpectedScoringDimensions(rubric)
}

func (ss *ScoringState) computeComposite(acc *scoringAccumulator, partial bool) scoringComposite {
	return ss.scoring.ComputeComposite(scoringAccumulatorInput{
		Rubric:   acc.Rubric,
		Expected: append([]string{}, acc.Expected...),
		Received: acc.Received,
		Partial:  partial,
	})
}

func (pc *FactoryPipelineCoordinator) computeComposite(acc *scoringAccumulator, partial bool) scoringComposite {
	return pc.scoringState.computeComposite(acc, partial)
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

func (pc *FactoryPipelineCoordinator) loadScoringSeed(ctx context.Context, verticalID string) (name, geography, mode string) {
	name, geography, mode, _, _ = pc.loadScoringSeedDetails(ctx, verticalID)
	return name, geography, mode
}

func scoringDefaultMode() string {
	if module := defaultWorkflowModuleOrNil(); module != nil {
		return defaultPipelineScanMode(module.SemanticSource())
	}
	return defaultPipelineScanMode(nil)
}

func (pc *FactoryPipelineCoordinator) handleVerticalDiscovered(ctx context.Context, evt events.Event) {
	pc.handleScoringRequested(withPipelineSourceAgent(ctx, ScoringNodeID), evt)
}

func (pc *FactoryPipelineCoordinator) loadScoringSeedDetails(ctx context.Context, verticalID string) (name, geography, mode, geographicScope string, discoveryContext map[string]any) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return "", "", "", "", nil
	}
	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil || !ok {
		return "", "", "", "", nil
	}
	name, geography = workflowInstanceIdentity(instance)
	rawMode := workflowInstanceDiscoveryMode(instance)
	rawSignals := workflowInstanceRawSignals(instance)
	if strings.TrimSpace(rawMode) == "" {
		rawMode = defaultPipelineScanMode(pc.SemanticSource())
	}
	if len(rawSignals) > 0 {
		geographicScope = pc.scoringState.scoring.NormalizeGeographicScope(asString(rawSignals["geographic_scope"]))
		discoveryContext = cloneMapFromAny(rawSignals["discovery_context"])
		if len(discoveryContext) == 0 && pc.scoringState != nil && pc.scoringState.scoring != nil {
			discoveryContext = pc.scoringState.scoring.BuildDiscoveryContextPayload(rawSignals)
		}
	}
	source := pc.SemanticSource()
	return strings.TrimSpace(name), strings.TrimSpace(geography), resolvePipelineScanMode(source, rawMode), geographicScope, discoveryContext
}

func (pc *FactoryPipelineCoordinator) handleScoringRequested(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	verticalID := workflowEventEntityIDWithPayload(evt, payload)
	if verticalID == "" {
		return
	}
	source := pc.SemanticSource()
	mode := resolvePipelineScanMode(source, asString(payload["mode"]))
	if mode == "" {
		_, _, dbMode := pc.loadScoringSeed(ctx, verticalID)
		mode = resolvePipelineScanMode(source, dbMode)
	}
	if mode == "" {
		mode = defaultPipelineScanMode(source)
	}
	rubric := pc.scoringState.scoring.SelectScoringRubric(mode)
	expected := pc.scoringState.scoring.ExpectedScoringDimensions(rubric)
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
		discoveryContext = pc.scoringState.scoring.BuildDiscoveryContextPayload(payload)
	}
	geographicScope := pc.scoringState.scoring.NormalizeGeographicScope(asString(payload["geographic_scope"]))
	acc := pc.scoringAccumulatorSnapshot(ctx, verticalID)
	if acc == nil {
		acc = &scoringAccumulator{
			EntityID:         verticalID,
			EntityName:       name,
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
	} else {
		// Keep existing progress but refresh metadata when discovery details improve.
		acc.EntityName = firstNonEmptyString(name, acc.EntityName)
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
	pc.mu.Lock()
	pc.scoringState.accumulators[verticalID] = acc
	pc.mu.Unlock()
	pc.persistWorkflowScoringAccumulator(ctx, acc)

	pc.applyWorkflowEventTransition(ctx, evt)

	scoringPayload := pc.payloadFactory.BuildScoringRequestedPayload(verticalID, acc)
	if excluded := pc.payloadFactory.DerivedScoringGeneratorAgent(ctx, acc); excluded != "" {
		scoringPayload["excluded_analysis_agent_id"] = excluded
		if assigned := pc.payloadFactory.SelectScoringAnalysisRecipient(excluded); assigned != "" {
			scoringPayload["assigned_analysis_agent_id"] = assigned
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
	signal := asFloat(payload["signal_strength"])
	if signal == 0 {
		// Keep compatibility with emit payloads using integer encoding.
		signal = float64(intFromAny(payload["signal_strength"]))
	}
	payload["signal_strength"] = signal

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
		_, geo, err := pc.loadEntityIdentity(ctx, parentID)
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
	if passed, reason := pc.evaluateScoringDerivedGuard(ctx, evt, payload); !passed {
		runtimeWarn("scoring-node", "dropping vertical.derived contract guard reject parent=%s reason=%s", parentID, reason)
		return
	}

	campaignID := strings.TrimSpace(asString(payload["campaign_id"]))
	verticalID, err := pc.ensureEntityDiscovered(ctx, name, geography, "derived", payload)
	if err != nil {
		log.Printf("scoring-node: ensure derived vertical failed parent=%s name=%s err=%v", parentID, name, err)
		return
	}
	discoveredPayload := payloadMap(pc.payloadFactory.BuildVerticalDiscoveredPayload(
		verticalID,
		name,
		geography,
		"derived",
		"", // scan_id (not applicable for derivation)
		campaignID,
		signal,
		strings.TrimSpace(evt.SourceAgent),
		payload,
	))
	pc.publish(ctx, "vertical.discovered", verticalID, discoveredPayload)
}

func (pc *FactoryPipelineCoordinator) evaluateScoringDerivedGuard(ctx context.Context, evt events.Event, payload map[string]any) (bool, string) {
	source := pc.SemanticSource()
	if pc == nil || source == nil {
		return true, ""
	}
	handler, ok := source.NodeEventHandler(ScoringNodeID, "vertical.derived")
	if !ok || handler.Guard == nil {
		return true, ""
	}
	entity := pc.scoringDerivedGuardEntity(ctx, payload)
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return false, "payload_encode_failed"
	}
	triggerCtx := workflowTriggerContext{
		Event: (events.Event{
			ID:          strings.TrimSpace(evt.ID),
			Type:        events.EventType("vertical.derived"),
			SourceAgent: strings.TrimSpace(evt.SourceAgent),
			Payload:     rawPayload,
		}).WithEntityID(strings.TrimSpace(firstNonEmptyString(asString(payload["parent_id"]), workflowEventEntityID(evt)))),
		State: WorkflowState{
			Stage:    "scoring",
			Metadata: entity,
		},
	}
	passed, evaluated := pc.evaluateWorkflowGuardSpec(triggerCtx, handler.Guard)
	if passed {
		return true, ""
	}
	if len(evaluated) == 0 {
		return false, "guard_failed"
	}
	return false, strings.TrimSpace(evaluated[len(evaluated)-1])
}

func (pc *FactoryPipelineCoordinator) scoringDerivedGuardEntity(ctx context.Context, payload map[string]any) map[string]any {
	entity := map[string]any{
		"generation_depth": intFromAny(payload["generation_depth"]),
	}
	parentID := strings.TrimSpace(asString(payload["parent_id"]))
	if children, err := pc.countDerivedChildren(ctx, parentID); err == nil {
		entity["child_count"] = children
	}
	prefilter := pc.scoringDerivedPrefilterDetail(payload)
	entity["red_flag_count"] = len(stringSliceFromAny(prefilter["red_flags"]))
	entity["evidence_count"] = len(stringSliceFromAny(prefilter["evidence_urls"]))
	entity["scores"] = map[string]any{
		"icp_crispness":          scoringDerivedBooleanScore(prefilter["passes_icp_gate"]),
		"retention_architecture": scoringDerivedBooleanScore(prefilter["passes_retention_gate"]),
	}
	return entity
}

func (pc *FactoryPipelineCoordinator) scoringDerivedPrefilterDetail(payload map[string]any) map[string]any {
	if pc == nil || pc.discoveryPolicy == nil {
		return map[string]any{}
	}
	signal := asFloat(payload["signal_strength"])
	if signal == 0 {
		signal = float64(intFromAny(payload["signal_strength"]))
	}
	detail := pc.discoveryPolicy.BuildPrefilterSkipDetail(payload, signal, signal, "", "derived")
	if detail == nil {
		return map[string]any{}
	}
	return detail
}

func scoringDerivedBooleanScore(raw any) int {
	if truthyMetadataFlag(raw) {
		return 100
	}
	return 0
}

func (pc *FactoryPipelineCoordinator) countDerivedChildren(ctx context.Context, parentID string) (int, error) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || strings.TrimSpace(parentID) == "" {
		return 0, nil
	}
	items, err := pc.workflowStore.List(ctx)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, item := range items {
		metadata := workflowMetadataSnapshot(item)
		if strings.TrimSpace(asString(metadata["instance_kind"])) != "vertical" {
			continue
		}
		if strings.TrimSpace(asString(metadata["parent_id"])) == strings.TrimSpace(parentID) {
			count++
		}
	}
	return count, nil
}

func (pc *FactoryPipelineCoordinator) updateScoredVerticalState(ctx context.Context, verticalID, stage string, payload map[string]any, reason string) {
	if pc == nil {
		return
	}
	pc.updateVerticalStage(ctx, verticalID, stage, "vertical.scored")
}

func (pc *FactoryPipelineCoordinator) handleScoreDimensionComplete(ctx context.Context, evt events.Event) {
	if pc == nil || pc.scoringState == nil {
		return
	}
	pc.scoringState.handleScoreDimensionComplete(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleScoringContestResolved(ctx context.Context, evt events.Event) {
	if pc == nil || pc.scoringState == nil {
		return
	}
	pc.scoringState.handleScoringContestResolved(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) finalizeScoringAccumulator(ctx context.Context, verticalID string, partial bool) {
	if pc == nil || pc.scoringState == nil {
		return
	}
	pc.scoringState.finalizeScoringAccumulator(ctx, verticalID, partial)
}

func (pc *FactoryPipelineCoordinator) checkScoringTimeouts(ctx context.Context, now time.Time) {
	if pc == nil || pc.scoringState == nil {
		return
	}
	pc.scoringState.checkTimeouts(ctx, now)
}

func (ss *ScoringState) handleScoreDimensionComplete(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	verticalID := workflowEventEntityIDWithPayload(evt, payload)
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

	ss.mu.Lock()
	acc := ss.accumulators[verticalID]
	if acc == nil {
		ss.mu.Unlock()
		if restored, ok := ss.runtime.loadWorkflowScoringAccumulator(ctx, verticalID); ok && restored != nil {
			ss.mu.Lock()
			if existing := ss.accumulators[verticalID]; existing == nil {
				ss.accumulators[verticalID] = restored
				acc = restored
			} else {
				acc = existing
			}
		} else {
			ss.mu.Lock()
		}
	}
	if acc == nil {
		acc = &scoringAccumulator{
			EntityID:        verticalID,
			Rubric:          "universal",
			Expected:        ss.scoring.ExpectedScoringDimensions("universal"),
			Received:        map[string]scoreDimensionResult{},
			Contested:       map[string]contestedDimension{},
			ContestNotified: map[string]bool{},
			RequestedAt:     time.Now().UTC(),
		}
		name, geo, mode := ss.runtime.loadScoringSeed(ctx, verticalID)
		acc.EntityName = name
		acc.Geography = geo
		acc.Mode = mode
		if acc.Mode == "" {
			acc.Mode = scoringDefaultMode()
		}
		acc.Rubric = ss.scoring.SelectScoringRubric(acc.Mode)
		acc.Expected = ss.scoring.ExpectedScoringDimensions(acc.Rubric)
		ss.accumulators[verticalID] = acc
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
			snapshot := cloneScoringAccumulator(acc)
			ss.mu.Unlock()
			ss.runtime.persistWorkflowScoringAccumulator(ctx, snapshot)
			ss.runtime.publish(ctx, "scoring.contested", verticalID, payloadMap(ss.payloadFactory.BuildScoringContestedPayload(verticalID, dim, contest, acc)))
			return
		}
		snapshot := cloneScoringAccumulator(acc)
		ss.mu.Unlock()
		ss.runtime.persistWorkflowScoringAccumulator(ctx, snapshot)
		return
	}

	acc.Received[dim] = next
	delete(acc.Contested, dim)
	delete(acc.ContestNotified, dim)
	acc.LastUpdatedAt = time.Now().UTC()
	snapshot := cloneScoringAccumulator(acc)
	ready := len(acc.Contested) == 0 && hasAllExpectedDimensions(acc)
	ss.mu.Unlock()
	ss.runtime.persistWorkflowScoringAccumulator(ctx, snapshot)

	if ready {
		ss.finalizeScoringAccumulator(ctx, verticalID, false)
	}
}

func (ss *ScoringState) handleScoringContestResolved(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	verticalID := workflowEventEntityIDWithPayload(evt, payload)
	dimension := strings.TrimSpace(asString(payload["dimension"]))
	if verticalID == "" || dimension == "" {
		return
	}
	resolved := clampScore100(intFromAny(payload["resolved_score"]))
	reasoning := strings.TrimSpace(asString(payload["reasoning"]))
	ss.mu.Lock()
	acc := ss.accumulators[verticalID]
	if acc == nil {
		ss.mu.Unlock()
		if restored, ok := ss.runtime.loadWorkflowScoringAccumulator(ctx, verticalID); ok && restored != nil {
			ss.mu.Lock()
			if existing := ss.accumulators[verticalID]; existing == nil {
				ss.accumulators[verticalID] = restored
				acc = restored
			} else {
				acc = existing
			}
		} else {
			return
		}
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
	snapshot := cloneScoringAccumulator(acc)
	ready := len(acc.Contested) == 0 && hasAllExpectedDimensions(acc)
	ss.mu.Unlock()
	ss.runtime.persistWorkflowScoringAccumulator(ctx, snapshot)
	if ready {
		ss.finalizeScoringAccumulator(ctx, verticalID, false)
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

func (ss *ScoringState) finalizeScoringAccumulator(ctx context.Context, verticalID string, partial bool) {
	ss.mu.Lock()
	acc := ss.accumulators[verticalID]
	if acc == nil {
		ss.mu.Unlock()
		return
	}
	if len(acc.Contested) > 0 {
		ss.mu.Unlock()
		return
	}
	if partial && len(acc.Received) == 0 {
		ss.mu.Unlock()
		return
	}
	result := ss.computeComposite(acc, partial || len(acc.Received) < len(acc.Expected))
	delete(ss.accumulators, verticalID)
	ss.mu.Unlock()
	ss.runtime.clearWorkflowScoringAccumulator(ctx, verticalID)

	scoredPayload := ss.payloadFactory.BuildVerticalScoredPayload(verticalID, result, acc)
	scoredPayloadMap := payloadMap(scoredPayload)
	ss.runtime.publish(ctx, "vertical.scored", verticalID, scoredPayloadMap)
	if outcome, ok := ss.runtime.applyWorkflowEventTransition(ctx, (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.scored"),
		SourceAgent: "scoring-node",
		Payload:     mustJSON(scoredPayloadMap),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(verticalID)); ok {
		if strings.TrimSpace(string(outcome.Transition.To)) == "killed" {
			ss.runtime.appendScoringDigestBuffer(ctx, scoredPayload)
		}
	} else if strings.EqualFold(strings.TrimSpace(result.Result), "rejected") {
		ss.runtime.appendScoringDigestBuffer(ctx, scoredPayload)
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
	pc.publish(ctx, "timer.portfolio_digest", workflowEventEntityID(evt), payloadMap(payload))
}

type scoringDigestEntry struct {
	ID           string
	EntityID     string
	EntityName   string
	Geography    string
	Result       string
	Reason       string
	Composite    float64
	Viability    float64
	ScoredAt     time.Time
}

func (pc *FactoryPipelineCoordinator) appendScoringDigestBuffer(ctx context.Context, scored map[string]any) {
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
		strings.TrimSpace(asString(scored["vertical_id"])),
		strings.TrimSpace(coalesce(strings.TrimSpace(asString(scored["vertical_name"])), strings.TrimSpace(asString(scored["vertical_id"])))),
		strings.TrimSpace(coalesce(strings.TrimSpace(asString(scored["geography"])), "unspecified")),
		asFloat(scored["composite_score"]),
		asFloat(scored["viability_score"]),
		strings.TrimSpace(coalesce(strings.TrimSpace(asString(scored["result"])), "rejected")),
		strings.TrimSpace(coalesce(strings.TrimSpace(asString(scored["reason"])), summary)),
	); err != nil {
		log.Printf("pipeline: append scoring digest buffer failed vertical=%s err=%v", strings.TrimSpace(asString(scored["vertical_id"])), err)
	}
}

func buildScoringRejectionSummary(scored map[string]any) string {
	name := strings.TrimSpace(asString(scored["vertical_name"]))
	if name == "" {
		name = strings.TrimSpace(asString(scored["vertical_id"]))
	}
	geography := strings.TrimSpace(asString(scored["geography"]))
	if geography == "" {
		geography = "unspecified"
	}
	reason := strings.TrimSpace(asString(scored["reason"]))
	if reason == "" {
		reason = "rejected"
	}
	return fmt.Sprintf(
		"%s (%s) rejected in scoring: reason=%s composite=%.2f viability=%.2f",
		name,
		geography,
		reason,
		asFloat(scored["composite_score"]),
		asFloat(scored["viability_score"]),
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
			&rec.EntityID,
			&rec.EntityName,
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
			coalesce(strings.TrimSpace(rec.EntityName), strings.TrimSpace(rec.EntityID)),
			coalesce(strings.TrimSpace(rec.Geography), "unspecified"),
			coalesce(strings.TrimSpace(rec.Reason), "rejected"),
			rec.Composite,
			rec.Viability,
		)
		out = append(out, map[string]any{
			"id":              rec.ID,
			"vertical_id":     rec.EntityID,
			"vertical_name":   rec.EntityName,
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
	enabled := pc.scoringDigestBufferEnabled
	pc.mu.Unlock()
	return enabled
}

func detectScoringDigestBuffer(ctx context.Context, db *sql.DB) bool {
	if db == nil {
		return false
	}
	var ok bool
	if err := dbQueryRowContext(ctx, db, `SELECT to_regclass('public.scoring_digest_buffer') IS NOT NULL`).Scan(&ok); err != nil {
		return false
	}
	return ok
}

func (ss *ScoringState) checkTimeouts(ctx context.Context, now time.Time) {
	if ss == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	ss.mu.Lock()
	stale := make([]string, 0, len(ss.accumulators))
	for verticalID, acc := range ss.accumulators {
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
			snapshot := cloneScoringAccumulator(acc)
			if snapshot != nil {
				ss.mu.Unlock()
				ss.runtime.persistWorkflowScoringAccumulator(ctx, snapshot)
				ss.mu.Lock()
				acc = ss.accumulators[verticalID]
				if acc == nil {
					continue
				}
			}
			stale = append(stale, verticalID)
		}
	}
	ss.mu.Unlock()
	for _, verticalID := range stale {
		ss.finalizeScoringAccumulator(ctx, verticalID, true)
	}
}
