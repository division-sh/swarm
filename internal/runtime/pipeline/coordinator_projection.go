package pipeline

import (
	"context"
	"sort"
	"strings"
	"time"

	"empireai/internal/events"
	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
	"github.com/google/uuid"
)

func (pc *FactoryPipelineCoordinator) publish(ctx context.Context, eventType, verticalID string, payload map[string]any) {
	if pc == nil {
		return
	}
	if payload == nil {
		payload = map[string]any{}
	}
	sourceAgent := pipelineSourceAgent(ctx)
	if sourceAgent == "" {
		sourceAgent = runtimeWorkflowID
	}
	emitted := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: sourceAgent,
		VerticalID:  strings.TrimSpace(verticalID),
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		*collector = append(*collector, emitted)
		return
	}
	if pc.bus == nil {
		runtimeWarn(
			runtimeWorkflowID,
			"dropping emit because event bus is nil event_type=%s vertical_id=%s",
			strings.TrimSpace(eventType),
			strings.TrimSpace(verticalID),
		)
		return
	}
	if err := pc.bus.Publish(ctx, emitted); err != nil {
		runtimeWarn(
			runtimeWorkflowID,
			"failed to publish runtime event event_type=%s event_id=%s vertical_id=%s: %v",
			strings.TrimSpace(eventType),
			strings.TrimSpace(emitted.ID),
			strings.TrimSpace(verticalID),
			err,
		)
	}
}

func (pc *FactoryPipelineCoordinator) publishDirect(ctx context.Context, eventType, verticalID string, payload map[string]any, recipients []string) {
	if pc == nil {
		return
	}
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		pc.publish(ctx, eventType, verticalID, payload)
		return
	}
	if payload == nil {
		payload = map[string]any{}
	}
	sourceAgent := pipelineSourceAgent(ctx)
	if sourceAgent == "" {
		sourceAgent = runtimeWorkflowID
	}
	emitted := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: sourceAgent,
		VerticalID:  strings.TrimSpace(verticalID),
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		*collector = append(*collector, emitted)
		return
	}
	if pc.bus == nil {
		runtimeWarn(
			runtimeWorkflowID,
			"dropping direct emit because event bus is nil event_type=%s vertical_id=%s recipients=%v",
			strings.TrimSpace(eventType),
			strings.TrimSpace(verticalID),
			recipients,
		)
		return
	}
	if err := pc.bus.PublishDirect(ctx, emitted, recipients); err != nil {
		runtimeWarn(
			runtimeWorkflowID,
			"failed to publish direct runtime event event_type=%s event_id=%s vertical_id=%s recipients=%v: %v",
			strings.TrimSpace(eventType),
			strings.TrimSpace(emitted.ID),
			strings.TrimSpace(verticalID),
			recipients,
			err,
		)
	}
}

func (vg *ValidationGate) getStateLocked(verticalID string) *validationPipelineState {
	st := vg.states[verticalID]
	if st == nil {
		st = &validationPipelineState{VerticalID: verticalID, Status: "active"}
		vg.states[verticalID] = st
	}
	if st.Status == "" {
		st.Status = "active"
	}
	return st
}

func (vg *ValidationGate) stageForState(st *validationPipelineState) string {
	if st == nil {
		return string(StageResearching)
	}
	switch strings.TrimSpace(st.Status) {
	case "packaged":
		return string(StageReadyForReview)
	case "parked":
		return string(StageReadyForReview)
	case "approved":
		return string(StageApproved)
	case "rejected":
		return string(StageKilled)
	}
	if !st.G1Research {
		return string(StageResearching)
	}
	if !st.G2Spec {
		if st.InnerRevisionCount > 0 {
			return string(StageSpecReview)
		}
		return string(StageMVPSpeccing)
	}
	if !st.G3CTO {
		return string(StageCTOSpecReview)
	}
	if !st.G4Brand {
		return string(StageBranding)
	}
	return string(StageBranding)
}

func workflowStateForVertical(verticalID, stage string, st *validationPipelineState) WorkflowState {
	state := WorkflowState{
		VerticalID: strings.TrimSpace(verticalID),
		Stage:      NormalizePipelineStage(stage),
	}
	if st == nil {
		return state
	}
	state.Status = strings.TrimSpace(st.Status)
	state.Metadata = map[string]any{
		"g1_research":           st.G1Research,
		"g2_spec":               st.G2Spec,
		"g3_cto":                st.G3CTO,
		"g4_brand":              st.G4Brand,
		"revision_count":        st.RevisionCount,
		"inner_revision_count":  st.InnerRevisionCount,
		"packaging_requested":   st.PackagingRequested,
		"packaging_retry_count": st.PackagingRetries,
		"spec_version":          st.SpecVersion,
	}
	return state
}

func (pc *FactoryPipelineCoordinator) updateVerticalStage(ctx context.Context, verticalID, stage, sourceEvent string) {
	if pc == nil || pc.db == nil {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	stage = strings.TrimSpace(string(NormalizePipelineStage(stage)))
	sourceEvent = strings.TrimSpace(sourceEvent)
	if verticalID == "" || stage == "" {
		return
	}
	defer pc.notifyTestVerticalStageUpdated(verticalID, stage)
	var currentStage string
	_ = dbQueryRowContext(ctx, pc.db, `
		SELECT COALESCE(stage, '')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&currentStage)
	to := NormalizePipelineStage(stage)
	workflowState := workflowStateForVertical(verticalID, currentStage, pc.validationStateSnapshot(verticalID))
	workflow := pc.WorkflowDefinition()
	if workflow != nil {
		if _, ok := workflow.Transition(workflowState, to); !ok && !workflow.CanTransition(workflowState, to) {
			runtimeWarn(
				runtimeWorkflowID,
				"non-canonical stage transition vertical_id=%s from=%s to=%s source_event=%s",
				verticalID,
				strings.TrimSpace(currentStage),
				stage,
				sourceEvent,
			)
		}
	}
	if stage == "ready_for_review" || stage == "marginal_review" {
		_, _ = dbExecContext(ctx, pc.db, `
			UPDATE verticals
			SET stage = $2,
			    parked_at = COALESCE(parked_at, now()),
			    updated_at = now()
			WHERE id = $1::uuid
		`, verticalID, stage)
		pc.persistWorkflowStageProjection(ctx, verticalID, currentStage, stage, sourceEvent, workflowState)
		return
	}
	if stage == "approved" {
		_, _ = dbExecContext(ctx, pc.db, `
			UPDATE verticals
			SET stage = $2,
			    approved_at = COALESCE(approved_at, now()),
			    parked_at = NULL,
			    updated_at = now()
			WHERE id = $1::uuid
		`, verticalID, stage)
		pc.persistWorkflowStageProjection(ctx, verticalID, currentStage, stage, sourceEvent, workflowState)
		return
	}
	if stage == "killed" {
		_, _ = dbExecContext(ctx, pc.db, `
			UPDATE verticals
			SET stage = $2,
			    kill_reason = CASE
					WHEN COALESCE(kill_reason,'') = '' THEN NULLIF($3,'')
					ELSE kill_reason
				END,
			    killed_at_stage = CASE
					WHEN COALESCE(killed_at_stage,'') = '' THEN NULLIF($3,'')
					ELSE killed_at_stage
				END,
			    updated_at = now()
			WHERE id = $1::uuid
		`, verticalID, stage, sourceEvent)
		pc.persistWorkflowStageProjection(ctx, verticalID, currentStage, stage, sourceEvent, workflowState)
		return
	}
	_, _ = dbExecContext(ctx, pc.db, `
		UPDATE verticals
		SET stage = $2,
		    updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, stage)
	pc.persistWorkflowStageProjection(ctx, verticalID, currentStage, stage, sourceEvent, workflowState)
}

func expectedAgents(mode string) int {
	if expected := runtimeproductpolicy.ExpectedScannerCount(mode); expected > 0 {
		return expected
	}
	return 1
}

func firstNonEmptyString(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func (pc *FactoryPipelineCoordinator) recordTransition(
	ctx context.Context,
	startedAt time.Time,
	eventType string,
	evt events.Event,
	payload map[string]any,
	before map[string]any,
	action string,
	dropReason string,
	emitted []events.Event,
	execErr error,
) {
	if pc == nil || pc.db == nil {
		return
	}
	pipelineType, pipelineID := pc.transitionIdentity(eventType, evt, payload)
	if pipelineID == "" {
		pipelineID = strings.TrimSpace(evt.ID)
	}
	if _, err := uuid.Parse(strings.TrimSpace(pipelineID)); err != nil {
		pipelineID = strings.TrimSpace(evt.ID)
	}
	after := pc.transitionStateSnapshot(eventType, evt, payload)
	emittedTypes := make([]string, 0, len(emitted))
	for _, out := range emitted {
		t := strings.TrimSpace(string(out.Type))
		if t != "" {
			emittedTypes = append(emittedTypes, t)
		}
	}
	errText := strings.TrimSpace(asString(execErr))
	if execErr != nil && errText == "" {
		errText = execErr.Error()
	}
	input := PipelineTransitionInput{
		EventID:       strings.TrimSpace(evt.ID),
		EventType:     eventType,
		Handler:       pc.runtimeHandlerID(eventType),
		PipelineType:  pipelineType,
		PipelineID:    pipelineID,
		Action:        strings.TrimSpace(action),
		StateBefore:   before,
		StateAfter:    after,
		EventsEmitted: emittedTypes,
		DropReason:    strings.TrimSpace(dropReason),
		Error:         errText,
		Duration:      time.Since(startedAt),
	}
	if AppendDeferredPipelineTransition(ctx, DeferredPipelineTransition{
		db:    pc.db,
		input: input,
	}) {
		return
	}
	_ = RecordPipelineTransition(ctx, pc.db, input)
}

func (pc *FactoryPipelineCoordinator) transitionIdentity(eventType string, evt events.Event, payload map[string]any) (pipelineType string, pipelineID string) {
	if isUUID(strings.TrimSpace(evt.VerticalID)) {
		return "validation", strings.TrimSpace(evt.VerticalID)
	}
	if v := strings.TrimSpace(asString(payload["vertical_id"])); isUUID(v) {
		return "validation", v
	}
	if v := strings.TrimSpace(asString(payload["campaign_id"])); isUUID(v) {
		return "campaign", v
	}
	if v := strings.TrimSpace(asString(payload["directive_id"])); isUUID(v) {
		return "campaign", v
	}
	if v := strings.TrimSpace(asString(payload["scan_id"])); isUUID(v) {
		return "scan", v
	}
	switch {
	case strings.HasPrefix(eventType, "scan."),
		strings.Contains(eventType, ".scan_"),
		eventType == "category.assessed",
		eventType == "trend.identified",
		eventType == "source.scraped",
		eventType == "dedup.resolved",
		eventType == "synthesis.resolved":
		pipelineType = "scan"
	default:
		pipelineType = "validation"
	}
	return pipelineType, strings.TrimSpace(evt.ID)
}

func (pc *FactoryPipelineCoordinator) transitionStateSnapshot(eventType string, evt events.Event, payload map[string]any) map[string]any {
	if pc == nil {
		return nil
	}
	out := map[string]any{
		"event_type":      strings.TrimSpace(eventType),
		"scans_active":    0,
		"scoring_active":  0,
		"pending_dedup":   0,
		"validations":     0,
		"processed_count": 0,
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		verticalID = strings.TrimSpace(asString(payload["vertical_id"]))
	}
	scanID := strings.TrimSpace(asString(payload["scan_id"]))
	if scanID == "" {
		scanID = strings.TrimSpace(evt.ID)
	}
	pc.mu.Lock()
	out["scans_active"] = len(pc.scanCoordinator.scans)
	out["scoring_active"] = len(pc.scoringState.accumulators)
	out["pending_dedup"] = len(pc.scanCoordinator.pendingDedup)
	out["validations"] = len(pc.validationGate.states)
	out["processed_count"] = len(pc.processed)
	var scanSnapshot *scanAccumulator
	if scanID != "" {
		if acc := pc.scanCoordinator.scans[scanID]; acc != nil {
			scanSnapshot = cloneScanAccumulator(acc)
		}
	}
	pc.mu.Unlock()
	pc.fillTransitionSnapshotWorkflowCounts(out)
	if verticalID != "" {
		if st := pc.validationStateSnapshot(verticalID); st != nil {
			out["validation_state"] = map[string]any{
				"vertical_id":          st.VerticalID,
				"status":               st.Status,
				"g1_research":          st.G1Research,
				"g2_spec":              st.G2Spec,
				"g3_cto":               st.G3CTO,
				"g4_brand":             st.G4Brand,
				"revision_count":       st.RevisionCount,
				"inner_revision_count": st.InnerRevisionCount,
				"spec_version":         st.SpecVersion,
				"packaging_requested":  st.PackagingRequested,
				"packaging_retries":    st.PackagingRetries,
			}
		}
	}
	if scanID != "" {
		if scanSnapshot == nil {
			if acc, _, ok := pc.loadWorkflowScanProjection(context.Background(), scanID); ok {
				scanSnapshot = acc
			}
		}
	}
	if scanSnapshot != nil {
		out["scan_state"] = map[string]any{
			"scan_id":              scanSnapshot.ScanID,
			"campaign_id":          scanSnapshot.CampaignID,
			"mode":                 scanSnapshot.Mode,
			"geography":            scanSnapshot.Geography,
			"reports_received":     scanSnapshot.Reports,
			"expected":             scanSnapshot.Expected,
			"completed_by":         len(scanSnapshot.CompletedBy),
			"verticals_discovered": scanSnapshot.Discovered,
			"verticals_skipped":    scanSnapshot.Skipped,
			"pending_dedup":        pc.pendingDedupCountForScan(scanSnapshot.ScanID),
		}
	}
	return out
}

func isUUID(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	_, err := uuid.Parse(raw)
	return err == nil
}

// Snapshot helpers for dashboard/tests.
func (pc *FactoryPipelineCoordinator) SnapshotScans() []map[string]any {
	pc.mu.Lock()
	out := make([]map[string]any, 0, len(pc.scanCoordinator.scans))
	ids := make([]string, 0, len(pc.scanCoordinator.scans))
	for id := range pc.scanCoordinator.scans {
		ids = append(ids, id)
	}
	pc.mu.Unlock()
	if len(ids) == 0 && pc.workflowStore != nil && pc.workflowStore.Enabled() {
		if items, err := pc.workflowStore.List(context.Background()); err == nil {
			for _, item := range items {
				acc, _, ok := restoreScanStateFromInstance(item)
				if !ok || acc == nil {
					continue
				}
				out = append(out, map[string]any{
					"scan_id":              acc.ScanID,
					"campaign_id":          acc.CampaignID,
					"mode":                 acc.Mode,
					"geography":            acc.Geography,
					"expected":             acc.Expected,
					"completed":            len(acc.CompletedBy),
					"reports":              acc.Reports,
					"verticals_discovered": acc.Discovered,
					"verticals_skipped":    acc.Skipped,
				})
			}
			sort.Slice(out, func(i, j int) bool { return asString(out[i]["scan_id"]) < asString(out[j]["scan_id"]) })
			return out
		}
	}
	sort.Strings(ids)
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, id := range ids {
		acc := pc.scanCoordinator.scans[id]
		out = append(out, map[string]any{
			"scan_id":              acc.ScanID,
			"campaign_id":          acc.CampaignID,
			"mode":                 acc.Mode,
			"geography":            acc.Geography,
			"expected":             acc.Expected,
			"completed":            len(acc.CompletedBy),
			"reports":              acc.Reports,
			"verticals_discovered": acc.Discovered,
			"verticals_skipped":    acc.Skipped,
		})
	}
	return out
}
