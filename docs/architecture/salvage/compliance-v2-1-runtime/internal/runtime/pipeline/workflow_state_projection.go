//go:build ignore

package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

const (
	workflowValidationBucket   = "validation-state"
	workflowScoringBucket      = "scoring-state"
	workflowScanBucket         = "scan-state"
	workflowPendingDedupBucket = "pending-dedup-state"

	workflowScanProjectionName         = "scan-campaign"
	workflowPendingDedupProjectionName = "pending-dedup"
)

func (pc *FactoryPipelineCoordinator) persistValidationWorkflowState(ctx context.Context, verticalID string) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return
	}
	st := pc.validationStateSnapshot(ctx, verticalID)
	template := pc.workflowInstanceTemplate(verticalID)
	if strings.TrimSpace(template.CurrentStage) == "" {
		template.CurrentStage = string(pc.currentWorkflowState(ctx, verticalID).Stage)
	}
	if strings.TrimSpace(template.CurrentStage) == "" {
		return
	}
	_, err := pc.workflowStore.Mutate(ctx, template, func(instance *WorkflowInstance) error {
		if instance.AccumulatorState == nil {
			instance.AccumulatorState = map[string]any{}
		}
		if st == nil {
			delete(instance.AccumulatorState, workflowValidationBucket)
			return nil
		}
		instance.AccumulatorState[workflowValidationBucket] = validationStateBucket(st)
		state := workflowStateForVertical(verticalID, instance.CurrentStage, st)
		if instance.Metadata == nil {
			instance.Metadata = map[string]any{}
		}
		for k, v := range state.Metadata {
			instance.Metadata[k] = v
		}
		if strings.TrimSpace(state.Status) != "" {
			instance.Metadata["status"] = strings.TrimSpace(state.Status)
		}
		return nil
	})
	if err != nil {
		runtimeWarn("pipeline-coordinator", "validation workflow projection failed vertical_id=%s: %v", verticalID, err)
	}
}

func (pc *FactoryPipelineCoordinator) persistScoringWorkflowState(ctx context.Context, verticalID string) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return
	}
	acc := pc.scoringAccumulatorSnapshot(ctx, verticalID)
	template := pc.workflowInstanceTemplate(verticalID)
	if strings.TrimSpace(template.CurrentStage) == "" {
		template.CurrentStage = string(pc.currentWorkflowState(ctx, verticalID).Stage)
	}
	if strings.TrimSpace(template.CurrentStage) == "" {
		return
	}
	_, err := pc.workflowStore.Mutate(ctx, template, func(instance *WorkflowInstance) error {
		if instance.AccumulatorState == nil {
			instance.AccumulatorState = map[string]any{}
		}
		if acc == nil {
			delete(instance.AccumulatorState, workflowScoringBucket)
			return nil
		}
		instance.AccumulatorState[workflowScoringBucket] = scoringAccumulatorBucket(acc)
		return nil
	})
	if err != nil {
		runtimeWarn("pipeline-coordinator", "scoring workflow projection failed vertical_id=%s: %v", verticalID, err)
	}
}

func validationStateBucket(st *validationPipelineState) map[string]any {
	if st == nil {
		return nil
	}
	out := map[string]any{
		"vertical_id":          strings.TrimSpace(st.VerticalID),
		"status":               strings.TrimSpace(st.Status),
		"g1_research":          st.G1Research,
		"g2_spec":              st.G2Spec,
		"g3_cto":               st.G3CTO,
		"g4_brand":             st.G4Brand,
		"research_payload":     parsePayloadMap(st.ResearchPayload),
		"spec_payload":         parsePayloadMap(st.SpecPayload),
		"cto_payload":          parsePayloadMap(st.CTOPayload),
		"brand_payload":        parsePayloadMap(st.BrandPayload),
		"scoring_payload":      parsePayloadMap(st.ScoringPayload),
		"revision_count":       st.RevisionCount,
		"inner_revision_count": st.InnerRevisionCount,
		"spec_version":         st.SpecVersion,
		"packaging_requested":  st.PackagingRequested,
		"packaging_retries":    st.PackagingRetries,
	}
	if st.PackagingRequestedAt != nil && !st.PackagingRequestedAt.IsZero() {
		out["packaging_requested_at"] = st.PackagingRequestedAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func validationStateFromBucket(verticalID string, raw any) *validationPipelineState {
	bucket, ok := asObject(raw)
	if !ok || len(bucket) == 0 {
		return nil
	}
	st := &validationPipelineState{
		VerticalID:         firstNonEmptyString(strings.TrimSpace(verticalID), strings.TrimSpace(asString(bucket["vertical_id"]))),
		Status:             strings.TrimSpace(asString(bucket["status"])),
		G1Research:         truthyMetadataFlag(bucket["g1_research"]),
		G2Spec:             truthyMetadataFlag(bucket["g2_spec"]),
		G3CTO:              truthyMetadataFlag(bucket["g3_cto"]),
		G4Brand:            truthyMetadataFlag(bucket["g4_brand"]),
		ResearchPayload:    mustJSON(asObjectOrEmpty(bucket["research_payload"])),
		SpecPayload:        mustJSON(asObjectOrEmpty(bucket["spec_payload"])),
		CTOPayload:         mustJSON(asObjectOrEmpty(bucket["cto_payload"])),
		BrandPayload:       mustJSON(asObjectOrEmpty(bucket["brand_payload"])),
		ScoringPayload:     mustJSON(asObjectOrEmpty(bucket["scoring_payload"])),
		RevisionCount:      asInt(bucket["revision_count"]),
		InnerRevisionCount: asInt(bucket["inner_revision_count"]),
		SpecVersion:        asInt(bucket["spec_version"]),
		PackagingRequested: truthyMetadataFlag(bucket["packaging_requested"]),
		PackagingRetries:   asInt(bucket["packaging_retries"]),
	}
	if st.Status == "" {
		st.Status = "active"
	}
	if requestedAt := parseWorkflowStoredTime(bucket["packaging_requested_at"]); !requestedAt.IsZero() {
		st.PackagingRequestedAt = &requestedAt
	}
	return st
}

func scoringAccumulatorBucket(acc *scoringAccumulator) map[string]any {
	if acc == nil {
		return nil
	}
	return map[string]any{
		"vertical_id":       strings.TrimSpace(acc.VerticalID),
		"vertical_name":     strings.TrimSpace(acc.VerticalName),
		"geography":         strings.TrimSpace(acc.Geography),
		"geographic_scope":  strings.TrimSpace(acc.GeographicScope),
		"mode":              strings.TrimSpace(acc.Mode),
		"rubric":            strings.TrimSpace(acc.Rubric),
		"discovery_context": cloneMap(acc.DiscoveryContext),
		"expected":          append([]string{}, acc.Expected...),
		"received":          acc.Received,
		"contested":         acc.Contested,
		"contest_notified":  acc.ContestNotified,
		"requested_at":      acc.RequestedAt.UTC().Format(time.RFC3339Nano),
		"last_updated_at":   acc.LastUpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func scoringAccumulatorFromBucket(verticalID string, raw any) *scoringAccumulator {
	bucket, ok := asObject(raw)
	if !ok || len(bucket) == 0 {
		return nil
	}
	acc := &scoringAccumulator{
		VerticalID:       firstNonEmptyString(strings.TrimSpace(verticalID), strings.TrimSpace(asString(bucket["vertical_id"]))),
		VerticalName:     strings.TrimSpace(asString(bucket["vertical_name"])),
		Geography:        strings.TrimSpace(asString(bucket["geography"])),
		GeographicScope:  strings.TrimSpace(asString(bucket["geographic_scope"])),
		Mode:             strings.TrimSpace(asString(bucket["mode"])),
		Rubric:           strings.TrimSpace(asString(bucket["rubric"])),
		DiscoveryContext: asObjectOrEmpty(bucket["discovery_context"]),
		Expected:         stringSliceFromAny(bucket["expected"]),
		Received:         map[string]scoreDimensionResult{},
		Contested:        map[string]contestedDimension{},
		ContestNotified:  map[string]bool{},
		RequestedAt:      parseWorkflowStoredTime(bucket["requested_at"]),
		LastUpdatedAt:    parseWorkflowStoredTime(bucket["last_updated_at"]),
	}
	_ = decodeWorkflowBucket(bucket["received"], &acc.Received)
	_ = decodeWorkflowBucket(bucket["contested"], &acc.Contested)
	_ = decodeWorkflowBucket(bucket["contest_notified"], &acc.ContestNotified)
	return acc
}

func scanAccumulatorBucket(acc *scanAccumulator) map[string]any {
	if acc == nil {
		return nil
	}
	completedBy := make([]string, 0, len(acc.CompletedBy))
	for agentID := range acc.CompletedBy {
		agentID = strings.TrimSpace(agentID)
		if agentID != "" {
			completedBy = append(completedBy, agentID)
		}
	}
	return map[string]any{
		"scan_id":      strings.TrimSpace(acc.ScanID),
		"campaign_id":  strings.TrimSpace(acc.CampaignID),
		"mode":         strings.TrimSpace(acc.Mode),
		"geography":    strings.TrimSpace(acc.Geography),
		"expected":     acc.Expected,
		"completed_by": completedBy,
		"report_data":  cloneReportData(acc.ReportData),
		"reports":      acc.Reports,
		"discovered":   acc.Discovered,
		"skipped":      acc.Skipped,
		"created_at":   acc.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func scanAccumulatorFromBucket(scanID string, raw any) *scanAccumulator {
	bucket, ok := asObject(raw)
	if !ok || len(bucket) == 0 {
		return nil
	}
	acc := &scanAccumulator{
		ScanID:      firstNonEmptyString(strings.TrimSpace(scanID), strings.TrimSpace(asString(bucket["scan_id"]))),
		CampaignID:  strings.TrimSpace(asString(bucket["campaign_id"])),
		Mode:        strings.TrimSpace(asString(bucket["mode"])),
		Geography:   strings.TrimSpace(asString(bucket["geography"])),
		Expected:    asInt(bucket["expected"]),
		CompletedBy: map[string]struct{}{},
		ReportData:  reportDataFromAny(bucket["report_data"]),
		Reports:     asInt(bucket["reports"]),
		Discovered:  asInt(bucket["discovered"]),
		Skipped:     asInt(bucket["skipped"]),
		CreatedAt:   parseWorkflowStoredTime(bucket["created_at"]),
	}
	for _, item := range stringSliceFromAny(bucket["completed_by"]) {
		acc.CompletedBy[item] = struct{}{}
	}
	return acc
}

func pendingDedupCandidateBucket(cand pendingCandidate) map[string]any {
	return map[string]any{
		"dedup_event_id": strings.TrimSpace(cand.DedupEventID),
		"existing_id":    strings.TrimSpace(cand.ExistingID),
		"scan_id":        strings.TrimSpace(cand.ScanID),
		"campaign_id":    strings.TrimSpace(cand.CampaignID),
		"mode":           strings.TrimSpace(cand.Mode),
		"geography":      strings.TrimSpace(cand.Geography),
		"name":           strings.TrimSpace(cand.Name),
		"signal":         cand.Signal,
		"payload":        cloneMap(cand.Payload),
	}
}

func pendingDedupCandidateFromBucket(dedupEventID string, raw any) (pendingCandidate, bool) {
	bucket, ok := asObject(raw)
	if !ok || len(bucket) == 0 {
		return pendingCandidate{}, false
	}
	return pendingCandidate{
		DedupEventID: firstNonEmptyString(strings.TrimSpace(dedupEventID), strings.TrimSpace(asString(bucket["dedup_event_id"]))),
		ExistingID:   strings.TrimSpace(asString(bucket["existing_id"])),
		ScanID:       strings.TrimSpace(asString(bucket["scan_id"])),
		CampaignID:   strings.TrimSpace(asString(bucket["campaign_id"])),
		Mode:         strings.TrimSpace(asString(bucket["mode"])),
		Geography:    strings.TrimSpace(asString(bucket["geography"])),
		Name:         strings.TrimSpace(asString(bucket["name"])),
		Signal:       asFloat(bucket["signal"]),
		Payload:      asObjectOrEmpty(bucket["payload"]),
	}, true
}

func decodeWorkflowBucket(raw any, dest any) bool {
	if raw == nil || dest == nil {
		return false
	}
	return json.Unmarshal(mustJSON(raw), dest) == nil
}

func parseWorkflowStoredTime(raw any) time.Time {
	switch typed := raw.(type) {
	case time.Time:
		return typed.UTC()
	case string:
		if ts, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(typed)); err == nil {
			return ts.UTC()
		}
		if ts, err := time.Parse(time.RFC3339, strings.TrimSpace(typed)); err == nil {
			return ts.UTC()
		}
	}
	return time.Time{}
}

func asObjectOrEmpty(raw any) map[string]any {
	if obj, ok := asObject(raw); ok {
		return obj
	}
	return map[string]any{}
}

func stringSliceFromAny(raw any) []string {
	switch typed := raw.(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if value := strings.TrimSpace(asString(item)); value != "" {
				out = append(out, value)
			}
		}
		return out
	default:
		return nil
	}
}

type workflowRuntimeProjectionSnapshot struct {
	Stages       map[string]string
	Validations  map[string]*validationPipelineState
	Scoring      map[string]*scoringAccumulator
	Scans        map[string]*scanAccumulator
	PendingDedup map[string]pendingCandidate
}

func (pc *FactoryPipelineCoordinator) workflowRuntimeProjectionSnapshot(ctx context.Context) workflowRuntimeProjectionSnapshot {
	out := workflowRuntimeProjectionSnapshot{
		Stages:       map[string]string{},
		Validations:  map[string]*validationPipelineState{},
		Scoring:      map[string]*scoringAccumulator{},
		Scans:        map[string]*scanAccumulator{},
		PendingDedup: map[string]pendingCandidate{},
	}
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return out
	}
	instances, err := pc.workflowStore.List(ctx)
	if err != nil {
		return out
	}
	workflowName := ""
	if bundle := pc.ContractBundle(); bundle != nil {
		workflowName = strings.TrimSpace(bundle.Workflow.Workflow.Name)
	}
	for _, instance := range instances {
		instanceID := strings.TrimSpace(instance.InstanceID)
		if instanceID == "" {
			continue
		}
		switch strings.TrimSpace(instance.WorkflowName) {
		case workflowName:
			if stage := strings.TrimSpace(instance.CurrentStage); stage != "" {
				out.Stages[instanceID] = stage
			}
			if validation := validationStateFromBucket(instanceID, instance.AccumulatorState[workflowValidationBucket]); validation != nil {
				out.Validations[instanceID] = validation
			}
			if scoring := scoringAccumulatorFromBucket(instanceID, instance.AccumulatorState[workflowScoringBucket]); scoring != nil {
				out.Scoring[instanceID] = scoring
			}
		case workflowScanProjectionName:
			if scan := scanAccumulatorFromBucket(instanceID, instance.AccumulatorState[workflowScanBucket]); scan != nil {
				out.Scans[instanceID] = scan
			}
		case workflowPendingDedupProjectionName:
			if cand, ok := pendingDedupCandidateFromBucket(instanceID, instance.AccumulatorState[workflowPendingDedupBucket]); ok {
				out.PendingDedup[instanceID] = cand
			}
		}
	}
	return out
}

func cloneReportData(in []map[string]any) []map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(in))
	for _, item := range in {
		out = append(out, cloneMap(item))
	}
	return out
}

func reportDataFromAny(raw any) []map[string]any {
	switch typed := raw.(type) {
	case []map[string]any:
		return cloneReportData(typed)
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			if obj, ok := asObject(item); ok {
				out = append(out, obj)
			}
		}
		return out
	default:
		return nil
	}
}

func (pc *FactoryPipelineCoordinator) syncOperationalWorkflowProjection(
	ctx context.Context,
	scans map[string]*scanAccumulator,
	pending map[string]pendingCandidate,
) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	version := "v2.1.0"
	if bundle := pc.ContractBundle(); bundle != nil {
		if got := strings.TrimSpace(bundle.Workflow.Workflow.Version); got != "" {
			version = got
		}
	}
	for scanID, acc := range scans {
		if acc == nil || strings.TrimSpace(scanID) == "" {
			continue
		}
		_ = pc.workflowStore.Upsert(ctx, WorkflowInstance{
			InstanceID:      strings.TrimSpace(scanID),
			WorkflowName:    workflowScanProjectionName,
			WorkflowVersion: version,
			CurrentStage:    "active",
			EnteredStageAt:  time.Now().UTC(),
			AccumulatorState: map[string]any{
				workflowScanBucket: scanAccumulatorBucket(acc),
			},
			Metadata: map[string]any{
				"projection_kind": "scan",
			},
		})
		pc.syncScanTimeoutProjectionSchedule(ctx, scanID, acc)
	}
	for dedupEventID, cand := range pending {
		if strings.TrimSpace(dedupEventID) == "" {
			continue
		}
		_ = pc.workflowStore.Upsert(ctx, WorkflowInstance{
			InstanceID:      strings.TrimSpace(dedupEventID),
			WorkflowName:    workflowPendingDedupProjectionName,
			WorkflowVersion: version,
			CurrentStage:    "pending",
			EnteredStageAt:  time.Now().UTC(),
			AccumulatorState: map[string]any{
				workflowPendingDedupBucket: pendingDedupCandidateBucket(cand),
			},
			Metadata: map[string]any{
				"projection_kind": "pending_dedup",
			},
		})
	}
	instances, err := pc.workflowStore.List(ctx)
	if err != nil {
		return
	}
	for _, instance := range instances {
		switch strings.TrimSpace(instance.WorkflowName) {
		case workflowScanProjectionName:
			if _, ok := scans[strings.TrimSpace(instance.InstanceID)]; !ok {
				pc.cancelScanTimeoutProjectionSchedule(ctx, instance.InstanceID)
				_ = pc.workflowStore.Delete(ctx, instance.InstanceID)
			}
		case workflowPendingDedupProjectionName:
			if _, ok := pending[strings.TrimSpace(instance.InstanceID)]; !ok {
				_ = pc.workflowStore.Delete(ctx, instance.InstanceID)
			}
		}
	}
}

func (pc *FactoryPipelineCoordinator) syncScanTimeoutProjectionSchedule(ctx context.Context, scanID string, acc *scanAccumulator) {
	if pc == nil || pc.scheduleStore == nil || acc == nil {
		return
	}
	scanID = strings.TrimSpace(scanID)
	if scanID == "" {
		return
	}
	createdAt := acc.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	payload := map[string]any{
		"scan_id":     scanID,
		"campaign_id": strings.TrimSpace(acc.CampaignID),
		"timer_id":    "scan_timeout",
		"trigger":     "timer.scan_timeout",
	}
	if geography := strings.TrimSpace(acc.Geography); geography != "" {
		payload["geography"] = geography
	}
	if mode := strings.TrimSpace(acc.Mode); mode != "" {
		payload["mode"] = mode
	}
	_ = pc.scheduleStore.UpsertSchedule(ctx, Schedule{
		AgentID:   workflowScanTimeoutAgentID(scanID),
		EventType: "timer.scan_timeout",
		Mode:      "once",
		At:        createdAt.Add(scanTimeout),
		Payload:   mustJSON(payload),
	})
}

func (pc *FactoryPipelineCoordinator) cancelScanTimeoutProjectionSchedule(ctx context.Context, scanID string) {
	if pc == nil || pc.scheduleStore == nil {
		return
	}
	scanID = strings.TrimSpace(scanID)
	if scanID == "" {
		return
	}
	_ = pc.scheduleStore.CancelSchedule(ctx, workflowScanTimeoutAgentID(scanID), "timer.scan_timeout")
}

func workflowScanTimeoutAgentID(scanID string) string {
	scanID = strings.TrimSpace(scanID)
	if scanID == "" {
		return "pipeline-coordinator:scan-timeout"
	}
	return "pipeline-coordinator:scan-timeout:" + scanID
}
