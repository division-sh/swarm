package pipeline

import (
	"context"
	"sort"
	"strings"
	"time"

	"empireai/internal/events"
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
		sourceAgent = "pipeline-coordinator"
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
			"pipeline-coordinator",
			"dropping emit because event bus is nil event_type=%s vertical_id=%s",
			strings.TrimSpace(eventType),
			strings.TrimSpace(verticalID),
		)
		return
	}
	if err := pc.bus.Publish(ctx, emitted); err != nil {
		runtimeWarn(
			"pipeline-coordinator",
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
		sourceAgent = "pipeline-coordinator"
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
			"pipeline-coordinator",
			"dropping direct emit because event bus is nil event_type=%s vertical_id=%s recipients=%v",
			strings.TrimSpace(eventType),
			strings.TrimSpace(verticalID),
			recipients,
		)
		return
	}
	if err := pc.bus.PublishDirect(ctx, emitted, recipients); err != nil {
		runtimeWarn(
			"pipeline-coordinator",
			"failed to publish direct runtime event event_type=%s event_id=%s vertical_id=%s recipients=%v: %v",
			strings.TrimSpace(eventType),
			strings.TrimSpace(emitted.ID),
			strings.TrimSpace(verticalID),
			recipients,
			err,
		)
	}
}

func (pc *FactoryPipelineCoordinator) getValidationStateLocked(verticalID string) *validationPipelineState {
	st := pc.validations[verticalID]
	if st == nil {
		st = &validationPipelineState{VerticalID: verticalID, Status: "active"}
		pc.validations[verticalID] = st
	}
	if st.Status == "" {
		st.Status = "active"
	}
	return st
}

func (pc *FactoryPipelineCoordinator) validationStageForState(st *validationPipelineState) string {
	if st == nil {
		return "researching"
	}
	switch strings.TrimSpace(st.Status) {
	case "packaged":
		return "ready_for_review"
	case "parked":
		return "ready_for_review"
	case "approved":
		return "approved"
	case "rejected":
		return "killed"
	}
	if !st.G1Research {
		return "researching"
	}
	if !st.G2Spec {
		if st.InnerRevisionCount > 0 {
			return "spec_review"
		}
		return "mvp_speccing"
	}
	if !st.G3CTO {
		return "cto_spec_review"
	}
	if !st.G4Brand {
		return "branding"
	}
	return "branding"
}

func (pc *FactoryPipelineCoordinator) updateVerticalStage(ctx context.Context, verticalID, stage, sourceEvent string) {
	if pc == nil || pc.db == nil {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	stage = strings.TrimSpace(stage)
	sourceEvent = strings.TrimSpace(sourceEvent)
	if verticalID == "" || stage == "" {
		return
	}
	if stage == "ready_for_review" {
		_, _ = dbExecContext(ctx, pc.db, `
			UPDATE verticals
			SET stage = $2,
			    parked_at = COALESCE(parked_at, now()),
			    updated_at = now()
			WHERE id = $1::uuid
		`, verticalID, stage)
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
		return
	}
	_, _ = dbExecContext(ctx, pc.db, `
		UPDATE verticals
		SET stage = $2,
		    updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, stage)
}

func expectedAgents(mode string) int {
	switch normalizeScanMode(mode) {
	case "automation_micro", "saas_gap", "saas_trend", "corpus":
		return 1
	case "local_services":
		return localServicesScannerExpected
	default:
		return 1
	}
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
		Handler:       "pipeline-coordinator",
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
	defer pc.mu.Unlock()
	out["scans_active"] = len(pc.scans)
	out["scoring_active"] = len(pc.scoring)
	out["pending_dedup"] = len(pc.pendingDedup)
	out["validations"] = len(pc.validations)
	out["processed_count"] = len(pc.processed)
	if verticalID != "" {
		if st := pc.validations[verticalID]; st != nil {
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
		if acc := pc.scans[scanID]; acc != nil {
			out["scan_state"] = map[string]any{
				"scan_id":              acc.ScanID,
				"campaign_id":          acc.CampaignID,
				"mode":                 acc.Mode,
				"geography":            acc.Geography,
				"reports_received":     acc.Reports,
				"expected":             acc.Expected,
				"completed_by":         len(acc.CompletedBy),
				"verticals_discovered": acc.Discovered,
				"verticals_skipped":    acc.Skipped,
				"pending_dedup":        pc.pendingDedupCountForScan(acc.ScanID),
			}
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
	defer pc.mu.Unlock()
	out := make([]map[string]any, 0, len(pc.scans))
	ids := make([]string, 0, len(pc.scans))
	for id := range pc.scans {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		acc := pc.scans[id]
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
