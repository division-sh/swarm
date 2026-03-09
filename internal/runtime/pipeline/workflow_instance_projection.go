package pipeline

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func (pc *FactoryPipelineCoordinator) persistWorkflowScoringAccumulator(ctx context.Context, acc *scoringAccumulator) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || acc == nil {
		return
	}
	verticalID := strings.TrimSpace(acc.VerticalID)
	if verticalID == "" {
		return
	}
	encoded := encodeScoringCompatibilityAccumulator(acc)
	restoreBucket := scoringRestoreDeltaBucket(pc)
	if pc != nil && pc.scoringState != nil && pc.scoringState.scoring != nil {
		encoded = pc.scoringState.scoring.EncodeScoringRestoreDelta((*ScoringAccumulator)(acc))
	}
	encodedNode := encodeScoringAccumulatorForWorkflow(pc.ContractBundle(), acc)
	_ = pc.workflowStore.Mutate(ctx, verticalID, func(instance *WorkflowInstance) {
		if instance.AccumulatorState == nil {
			instance.AccumulatorState = map[string]any{}
		}
		instance.AccumulatorState[restoreBucket] = encoded
		instance.AccumulatorState["scoring-node"] = encodedNode
	})
}

func (pc *FactoryPipelineCoordinator) clearWorkflowScoringAccumulator(ctx context.Context, verticalID string) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return
	}
	_ = pc.workflowStore.Mutate(ctx, verticalID, func(instance *WorkflowInstance) {
		if instance.AccumulatorState == nil {
			return
		}
		delete(instance.AccumulatorState, scoringRestoreDeltaBucket(pc))
		delete(instance.AccumulatorState, "scoring-node")
	})
}

func (pc *FactoryPipelineCoordinator) loadWorkflowScoringAccumulator(ctx context.Context, verticalID string) (*scoringAccumulator, bool) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil, false
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return nil, false
	}
	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil || !ok {
		return nil, false
	}
	acc, restored := restoreScoringAccumulatorFromInstance(instance)
	if !restored || acc == nil {
		return nil, false
	}
	pc.hydrateWorkflowScoringAccumulator(ctx, acc)
	return acc, true
}

func (pc *FactoryPipelineCoordinator) scoringAccumulatorSnapshot(ctx context.Context, verticalID string) *scoringAccumulator {
	if pc == nil {
		return nil
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return nil
	}
	pc.mu.Lock()
	acc := cloneScoringAccumulator(pc.scoringState.accumulators[verticalID])
	pc.mu.Unlock()
	if acc != nil {
		return acc
	}
	if restored, ok := pc.loadWorkflowScoringAccumulator(ctx, verticalID); ok && restored != nil {
		return restored
	}
	return nil
}

func (pc *FactoryPipelineCoordinator) hydrateWorkflowScoringAccumulator(ctx context.Context, acc *scoringAccumulator) {
	if pc == nil || acc == nil {
		return
	}
	name, geography, mode, geographicScope, discoveryContext := pc.loadScoringSeedDetails(ctx, acc.VerticalID)
	acc.VerticalName = firstNonEmptyString(acc.VerticalName, name)
	acc.Geography = firstNonEmptyString(acc.Geography, geography)
	acc.GeographicScope = firstNonEmptyString(acc.GeographicScope, geographicScope)
	acc.Mode = firstNonEmptyString(normalizeScanMode(acc.Mode), normalizeScanMode(mode))
	if acc.Mode == "" {
		acc.Mode = "saas_gap"
	}
	if len(acc.DiscoveryContext) == 0 {
		acc.DiscoveryContext = cloneMap(discoveryContext)
	}
	if strings.TrimSpace(acc.Rubric) == "" && pc.scoringState != nil && pc.scoringState.scoring != nil {
		acc.Rubric = pc.scoringState.scoring.SelectScoringRubric(acc.Mode)
	}
	if len(acc.Expected) == 0 && pc.scoringState != nil && pc.scoringState.scoring != nil {
		acc.Expected = pc.scoringState.scoring.ExpectedScoringDimensions(acc.Rubric)
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
}

func (pc *FactoryPipelineCoordinator) persistWorkflowScanProjection(ctx context.Context, scans map[string]*scanAccumulator, pending map[string]pendingCandidate) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	pendingByScan := map[string][]pendingCandidate{}
	for _, cand := range pending {
		scanID := strings.TrimSpace(cand.ScanID)
		if scanID == "" {
			continue
		}
		pendingByScan[scanID] = append(pendingByScan[scanID], cand)
	}
	for scanID, acc := range scans {
		scanID = strings.TrimSpace(scanID)
		if scanID == "" || acc == nil {
			continue
		}
		encodedScan := encodeScanAccumulatorForWorkflow(pc.ContractBundle(), acc)
		encodedPending := encodePendingCandidatesForWorkflow(pc.ContractBundle(), pendingByScan[scanID])
		encodedDiscovery := encodeDiscoveryStateForWorkflow(pc.ContractBundle(), pendingByScan[scanID])
		bundle := pc.ContractBundle()
		_ = pc.workflowStore.Mutate(ctx, scanID, func(instance *WorkflowInstance) {
			if strings.TrimSpace(instance.WorkflowName) == "" {
				instance.WorkflowName = strings.TrimSpace(bundle.Workflow.Workflow.Name)
			}
			if strings.TrimSpace(instance.WorkflowVersion) == "" {
				instance.WorkflowVersion = strings.TrimSpace(bundle.Workflow.Workflow.Version)
			}
			if strings.TrimSpace(instance.CurrentStage) == "" {
				instance.CurrentStage = "scanning"
			}
			if instance.AccumulatorState == nil {
				instance.AccumulatorState = map[string]any{}
			}
			instance.AccumulatorState["scan-state"] = encodedScan
			instance.AccumulatorState["pending-dedup"] = encodedPending
			instance.AccumulatorState["discovery-aggregator"] = encodedDiscovery
			if instance.Metadata == nil {
				instance.Metadata = map[string]any{}
			}
			instance.Metadata["instance_kind"] = "scan"
		})
	}
	items, err := pc.workflowStore.List(ctx)
	if err != nil {
		return
	}
	for _, item := range items {
		acc, _, ok := restoreScanStateFromInstance(item)
		if !ok || acc == nil {
			continue
		}
		if _, stillActive := scans[strings.TrimSpace(acc.ScanID)]; stillActive {
			continue
		}
		_ = pc.workflowStore.Delete(ctx, item.InstanceID)
	}
}

func (pc *FactoryPipelineCoordinator) loadWorkflowScanProjection(ctx context.Context, scanID string) (*scanAccumulator, map[string]pendingCandidate, bool) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil, nil, false
	}
	scanID = strings.TrimSpace(scanID)
	if scanID == "" {
		return nil, nil, false
	}
	instance, ok, err := pc.workflowStore.Load(ctx, scanID)
	if err != nil || !ok {
		return nil, nil, false
	}
	return restoreScanStateFromInstance(instance)
}

func restoreValidationStateFromInstance(instance WorkflowInstance) (*validationPipelineState, bool) {
	metadata := cloneStringAnyMap(instance.Metadata)
	if bucket, ok := asObject(instance.AccumulatorState["validation-orchestrator"]); ok {
		if gateState, ok := asObject(bucket["gate_state"]); ok {
			for _, gate := range []string{"g1_research", "g2_spec", "g3_cto", "g4_brand"} {
				if _, exists := metadata[gate]; !exists {
					metadata[gate] = gateState[gate]
				}
			}
		}
		if _, exists := metadata["revision_count"]; !exists {
			metadata["revision_count"] = bucket["revision_count"]
		}
	}
	if len(metadata) == 0 {
		return nil, false
	}
	bucket, _ := asObject(instance.AccumulatorState["validation-orchestrator"])
	verticalID := strings.TrimSpace(instance.InstanceID)
	if verticalID == "" {
		return nil, false
	}
	st := &validationPipelineState{
		VerticalID:         verticalID,
		Status:             strings.TrimSpace(asString(metadata["status"])),
		G1Research:         truthyMetadataFlag(metadata["g1_research"]),
		G2Spec:             truthyMetadataFlag(metadata["g2_spec"]),
		G3CTO:              truthyMetadataFlag(metadata["g3_cto"]),
		G4Brand:            truthyMetadataFlag(metadata["g4_brand"]),
		RevisionCount:      asInt(metadata["revision_count"]),
		InnerRevisionCount: asInt(metadata["inner_revision_count"]),
		SpecVersion:        asInt(metadata["spec_version"]),
		PackagingRequested: truthyMetadataFlag(metadata["packaging_requested"]),
		PackagingRetries:   asInt(metadata["packaging_retry_count"]),
	}
	if raw, ok := bucket["packaging_requested_at"]; ok {
		if ts := parseWorkflowTime(raw); !ts.IsZero() {
			t := ts
			st.PackagingRequestedAt = &t
		}
	}
	assignJSONRaw(&st.ResearchPayload, bucket["research_payload"])
	assignJSONRaw(&st.SpecPayload, bucket["spec_payload"])
	assignJSONRaw(&st.CTOPayload, bucket["cto_payload"])
	assignJSONRaw(&st.BrandPayload, bucket["brand_payload"])
	assignJSONRaw(&st.ScoringPayload, bucket["scoring_payload"])
	if st.Status == "" && (st.G1Research || st.G2Spec || st.G3CTO || st.G4Brand || st.RevisionCount > 0 || st.SpecVersion > 0) {
		st.Status = "active"
	}
	if st.Status == "" {
		return nil, false
	}
	return st, true
}

func restoreScoringAccumulatorFromInstance(instance WorkflowInstance) (*scoringAccumulator, bool) {
	nodeBucket, nodeOK := asObject(instance.AccumulatorState["scoring-node"])
	compatBucket, compatOK := scoringRestoreDeltaFromInstance(instance)
	if !nodeOK || len(nodeBucket) == 0 {
		return nil, false
	}
	verticalID := strings.TrimSpace(firstNonEmptyString(asString(nodeBucket["vertical_id"]), instance.InstanceID))
	if verticalID == "" {
		return nil, false
	}
	acc := &scoringAccumulator{
		VerticalID:       verticalID,
		Expected:        stringSliceFromAny(nodeBucket["dimensions_requested"]),
		Received:        decodeScoreDimensionResults(nodeBucket["dimensions_received"]),
		RequestedAt:     parseWorkflowTime(nodeBucket["started_at"]),
		LastUpdatedAt:   parseWorkflowTime(nodeBucket["completed_at"]),
		Contested:       map[string]contestedDimension{},
		ContestNotified: map[string]bool{},
	}
	if compatOK && len(compatBucket) > 0 {
		applyScoringCompatibilityDelta(acc, compatBucket)
	}
	if acc.Mode == "" && acc.Rubric == "" && len(acc.Expected) == 0 && len(acc.Received) == 0 && len(acc.Contested) == 0 {
		return nil, false
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
	return acc, true
}

func scoringRestoreDeltaBucket(pc *FactoryPipelineCoordinator) string {
	if pc != nil && pc.scoringState != nil && pc.scoringState.scoring != nil {
		if bucket := strings.TrimSpace(pc.scoringState.scoring.ScoringRestoreDeltaBucket()); bucket != "" {
			return bucket
		}
	}
	if module := defaultWorkflowModuleOrNil(); module != nil && module.ScoringPolicy() != nil {
		if bucket := strings.TrimSpace(module.ScoringPolicy().ScoringRestoreDeltaBucket()); bucket != "" {
			return bucket
		}
	}
	return "scoring-restore"
}

func scoringRestoreDeltaFromInstance(instance WorkflowInstance) (map[string]any, bool) {
	if bucket, ok := asObject(instance.AccumulatorState["scoring-restore"]); ok && len(bucket) > 0 {
		return bucket, true
	}
	return nil, false
}

func applyScoringCompatibilityDelta(acc *scoringAccumulator, compatBucket map[string]any) {
	if acc == nil || len(compatBucket) == 0 {
		return
	}
	if module := defaultWorkflowModuleOrNil(); module != nil && module.ScoringPolicy() != nil {
		module.ScoringPolicy().ApplyScoringRestoreDelta((*ScoringAccumulator)(acc), compatBucket)
		return
	}
	ApplyScoringRestoreDelta((*ScoringAccumulator)(acc), compatBucket)
}

func scoringNodeProjectionPresent(instance WorkflowInstance) bool {
	nodeBucket, ok := asObject(instance.AccumulatorState["scoring-node"])
	if !ok || len(nodeBucket) == 0 {
		return false
	}
	verticalID := strings.TrimSpace(firstNonEmptyString(asString(nodeBucket["vertical_id"]), instance.InstanceID))
	if verticalID == "" {
		return false
	}
	return len(stringSliceFromAny(nodeBucket["dimensions_requested"])) > 0 || len(asObjectLoose(nodeBucket["dimensions_received"])) > 0
}

func asObjectLoose(value any) map[string]any {
	out, _ := asObject(value)
	return out
}

func restoreScanStateFromInstance(instance WorkflowInstance) (*scanAccumulator, map[string]pendingCandidate, bool) {
	bucket, ok := asObject(instance.AccumulatorState["scan-state"])
	if !ok || len(bucket) == 0 {
		return nil, nil, false
	}
	scanID := strings.TrimSpace(firstNonEmptyString(asString(bucket["scan_id"]), instance.InstanceID))
	if scanID == "" {
		return nil, nil, false
	}
	acc := &scanAccumulator{
		ScanID:      scanID,
		CampaignID:  strings.TrimSpace(asString(bucket["campaign_id"])),
		Mode:        normalizeScanMode(asString(bucket["mode"])),
		Geography:   strings.TrimSpace(asString(bucket["geography"])),
		Expected:    maxInt(asInt(bucket["expected"]), len(stringSliceFromAny(firstNonNil(bucket["expected_scanners"], bucket["expected"])))),
		Reports:     asInt(bucket["reports"]),
		Discovered:  asInt(bucket["discovered"]),
		Skipped:     asInt(bucket["skipped"]),
		CreatedAt:   parseWorkflowTime(firstNonNil(bucket["started_at"], bucket["created_at"])),
		CompletedBy: map[string]struct{}{},
		ReportData:  []map[string]any{},
	}
	for _, key := range stringSliceFromAny(firstNonNil(bucket["completed_scanners"], bucket["completed_by"])) {
		acc.CompletedBy[strings.TrimSpace(key)] = struct{}{}
	}
	for _, item := range sliceOfMapsFromAny(bucket["report_data"]) {
		acc.ReportData = append(acc.ReportData, item)
	}
	pending := map[string]pendingCandidate{}
	for _, raw := range sliceOfMapsFromAny(instance.AccumulatorState["pending-dedup"]) {
		cand := pendingCandidate{
			DedupEventID: strings.TrimSpace(firstNonEmptyString(asString(raw["dedup_event_id"]), asString(raw["id"]))),
			ExistingID:   strings.TrimSpace(firstNonEmptyString(asString(raw["existing_id"]), asString(raw["matched_vertical_id"]))),
			ScanID:       strings.TrimSpace(firstNonEmptyString(asString(raw["scan_id"]), scanID)),
			CampaignID:   strings.TrimSpace(asString(raw["campaign_id"])),
			Mode:         normalizeScanMode(asString(raw["mode"])),
			Geography:    strings.TrimSpace(asString(raw["geography"])),
			Name:         strings.TrimSpace(firstNonEmptyString(asString(raw["name"]), asString(raw["opportunity_name"]))),
			Signal:       asFloat(firstNonNil(raw["signal"], raw["signal_strength"])),
			Payload:      cloneMapFromAny(firstNonNil(raw["payload"], raw["evidence"])),
		}
		if cand.DedupEventID != "" {
			pending[cand.DedupEventID] = cand
		}
	}
	return acc, pending, true
}

func encodeScoringAccumulator(acc *scoringAccumulator) map[string]any {
	if acc == nil {
		return map[string]any{}
	}
	out := map[string]any{
		"vertical_id":       strings.TrimSpace(acc.VerticalID),
		"vertical_name":     strings.TrimSpace(acc.VerticalName),
		"geography":         strings.TrimSpace(acc.Geography),
		"geographic_scope":  strings.TrimSpace(acc.GeographicScope),
		"mode":              strings.TrimSpace(acc.Mode),
		"rubric":            strings.TrimSpace(acc.Rubric),
		"discovery_context": cloneMap(acc.DiscoveryContext),
		"expected":          append([]string(nil), acc.Expected...),
		"received":          encodeScoreDimensionResults(acc.Received),
		"contested":         encodeContestedDimensions(acc.Contested),
		"contest_notified":  cloneBoolMap(acc.ContestNotified),
	}
	if !acc.RequestedAt.IsZero() {
		out["requested_at"] = acc.RequestedAt.UTC().Format(time.RFC3339Nano)
	}
	if !acc.LastUpdatedAt.IsZero() {
		out["last_updated_at"] = acc.LastUpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func encodeScoringCompatibilityAccumulator(acc *scoringAccumulator) map[string]any {
	if acc == nil {
		return map[string]any{}
	}
	return EncodeScoringRestoreDelta((*ScoringAccumulator)(acc))
}

func EncodeScoringRestoreDelta(acc *ScoringAccumulator) map[string]any {
	if acc == nil {
		return map[string]any{}
	}
	out := map[string]any{
		"contested":        encodeContestedDimensions(acc.Contested),
		"contest_notified": cloneBoolMap(acc.ContestNotified),
	}
	return out
}

func ApplyScoringRestoreDelta(acc *ScoringAccumulator, compatBucket map[string]any) {
	if acc == nil || len(compatBucket) == 0 {
		return
	}
	if len(acc.Contested) == 0 {
		acc.Contested = decodeContestedDimensions(compatBucket["contested"])
	}
	if len(acc.ContestNotified) == 0 {
		acc.ContestNotified = boolMapFromAny(compatBucket["contest_notified"])
	}
}

func encodeScoringAccumulatorForWorkflow(bundle *runtimecontracts.WorkflowContractBundle, acc *scoringAccumulator) map[string]any {
	fields := workflowSystemNodeStateSchemaFields(bundle, ScoringNodeID)
	if len(fields) == 0 || acc == nil {
		return map[string]any{}
	}
	out := map[string]any{}
	if _, ok := fields["vertical_id"]; ok {
		out["vertical_id"] = strings.TrimSpace(acc.VerticalID)
	}
	if _, ok := fields["dimensions_requested"]; ok {
		out["dimensions_requested"] = append([]string(nil), acc.Expected...)
	}
	if _, ok := fields["dimensions_received"]; ok {
		out["dimensions_received"] = encodeScoreDimensionResults(acc.Received)
	}
	if _, ok := fields["analyst_id"]; ok {
		out["analyst_id"] = strings.TrimSpace(asString(acc.DiscoveryContext["analyst_id"]))
	}
	if _, ok := fields["started_at"]; ok && !acc.RequestedAt.IsZero() {
		out["started_at"] = acc.RequestedAt.UTC().Format(time.RFC3339Nano)
	}
	if _, ok := fields["completed_at"]; ok && len(acc.Expected) > 0 && len(acc.Received) >= len(acc.Expected) && !acc.LastUpdatedAt.IsZero() {
		out["completed_at"] = acc.LastUpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func encodeScanAccumulator(acc *scanAccumulator) map[string]any {
	if acc == nil {
		return map[string]any{}
	}
	completedBy := make([]string, 0, len(acc.CompletedBy))
	for key := range acc.CompletedBy {
		completedBy = append(completedBy, strings.TrimSpace(key))
	}
	sort.Strings(completedBy)
	reportData := make([]map[string]any, 0, len(acc.ReportData))
	for _, item := range acc.ReportData {
		reportData = append(reportData, cloneMap(item))
	}
	out := map[string]any{
		"scan_id":      strings.TrimSpace(acc.ScanID),
		"campaign_id":  strings.TrimSpace(acc.CampaignID),
		"mode":         strings.TrimSpace(acc.Mode),
		"geography":    strings.TrimSpace(acc.Geography),
		"expected":     acc.Expected,
		"reports":      acc.Reports,
		"discovered":   acc.Discovered,
		"skipped":      acc.Skipped,
		"completed_by": completedBy,
		"report_data":  reportData,
	}
	if !acc.CreatedAt.IsZero() {
		out["created_at"] = acc.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func encodeScanAccumulatorForWorkflow(bundle *runtimecontracts.WorkflowContractBundle, acc *scanAccumulator) map[string]any {
	out := encodeScanAccumulator(acc)
	if acc == nil {
		return out
	}
	stateFields := workflowSystemNodeStateSchemaFields(bundle, "scan-orchestrator")
	if len(stateFields) == 0 {
		return out
	}
	expectedScanners := scanExpectedDispatchKeys(bundle, acc)
	completedScanners := make([]string, 0, len(acc.CompletedBy))
	for key := range acc.CompletedBy {
		key = strings.TrimSpace(key)
		if key != "" {
			completedScanners = append(completedScanners, key)
		}
	}
	sort.Strings(completedScanners)
	projected := map[string]any{}
	if _, ok := stateFields["scan_id"]; ok {
		projected["scan_id"] = strings.TrimSpace(acc.ScanID)
	}
	if _, ok := stateFields["campaign_id"]; ok {
		projected["campaign_id"] = strings.TrimSpace(acc.CampaignID)
	}
	if _, ok := stateFields["geography"]; ok {
		projected["geography"] = strings.TrimSpace(acc.Geography)
	}
	if _, ok := stateFields["mode"]; ok {
		projected["mode"] = strings.TrimSpace(acc.Mode)
	}
	if _, ok := stateFields["expected_scanners"]; ok {
		projected["expected_scanners"] = expectedScanners
	}
	if _, ok := stateFields["completed_scanners"]; ok {
		projected["completed_scanners"] = completedScanners
	}
	if _, ok := stateFields["started_at"]; ok && !acc.CreatedAt.IsZero() {
		projected["started_at"] = acc.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	if _, ok := stateFields["status"]; ok {
		projected["status"] = scanProjectionStatus(acc, expectedScanners)
	}
	return projected
}

func scanExpectedDispatchKeys(bundle *runtimecontracts.WorkflowContractBundle, acc *scanAccumulator) []string {
	if acc == nil {
		return nil
	}
	expected := scanDispatchKeysForMode(bundle, acc.Mode)
	if len(expected) >= maxInt(acc.Expected, 0) {
		return expected
	}
	for idx := len(expected); idx < maxInt(acc.Expected, 0); idx++ {
		expected = append(expected, "slot:"+strconv.Itoa(idx+1))
	}
	return expected
}

func scanDispatchKeysForMode(bundle *runtimecontracts.WorkflowContractBundle, mode string) []string {
	if bundle == nil {
		return nil
	}
	node, ok := bundle.Nodes["scan-orchestrator"]
	if !ok {
		return nil
	}
	handler, ok := node.EventHandlers["scan.requested"]
	if !ok {
		return nil
	}
	raw, ok := handler.ModeToScanners[normalizeScanMode(mode)]
	if !ok {
		return nil
	}
	return stringSliceFromAny(raw)
}

func scanProjectionStatus(acc *scanAccumulator, expectedScanners []string) string {
	if acc == nil {
		return ""
	}
	expected := maxInt(acc.Expected, len(expectedScanners))
	if expected <= 0 {
		expected = 1
	}
	if len(acc.CompletedBy) >= expected {
		return "completed"
	}
	return "active"
}

func firstNonNil(vals ...any) any {
	for _, v := range vals {
		if v != nil {
			return v
		}
	}
	return nil
}

func encodePendingCandidates(candidates []pendingCandidate) []map[string]any {
	if len(candidates) == 0 {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(candidates))
	for _, cand := range candidates {
		out = append(out, map[string]any{
			"dedup_event_id": strings.TrimSpace(cand.DedupEventID),
			"existing_id":    strings.TrimSpace(cand.ExistingID),
			"scan_id":        strings.TrimSpace(cand.ScanID),
			"campaign_id":    strings.TrimSpace(cand.CampaignID),
			"mode":           strings.TrimSpace(cand.Mode),
			"geography":      strings.TrimSpace(cand.Geography),
			"name":           strings.TrimSpace(cand.Name),
			"signal":         cand.Signal,
			"payload":        cloneMap(cand.Payload),
		})
	}
	return out
}

func encodePendingCandidatesForWorkflow(bundle *runtimecontracts.WorkflowContractBundle, candidates []pendingCandidate) []map[string]any {
	out := encodePendingCandidates(candidates)
	fields := workflowSystemNodeStateSchemaFields(bundle, "discovery-aggregator")
	if len(fields) == 0 {
		return out
	}
	for idx := range out {
		cand := candidates[idx]
		item := out[idx]
		if _, ok := fields["id"]; ok && strings.TrimSpace(asString(item["id"])) == "" {
			item["id"] = strings.TrimSpace(cand.DedupEventID)
		}
		if _, ok := fields["opportunity_name"]; ok && strings.TrimSpace(asString(item["opportunity_name"])) == "" {
			item["opportunity_name"] = strings.TrimSpace(cand.Name)
		}
		if _, ok := fields["signal_strength"]; ok {
			item["signal_strength"] = cand.Signal
		}
		if _, ok := fields["evidence"]; ok && item["evidence"] == nil {
			item["evidence"] = cloneMap(cand.Payload)
		}
		if _, ok := fields["status"]; ok && strings.TrimSpace(asString(item["status"])) == "" {
			item["status"] = "pending"
		}
		if _, ok := fields["matched_vertical_id"]; ok && strings.TrimSpace(asString(item["matched_vertical_id"])) == "" {
			item["matched_vertical_id"] = strings.TrimSpace(cand.ExistingID)
		}
		if _, ok := fields["created_at"]; ok && item["created_at"] == nil {
			if createdAt := parseWorkflowTime(cand.Payload["created_at"]); !createdAt.IsZero() {
				item["created_at"] = createdAt.UTC().Format(time.RFC3339Nano)
			}
		}
		out[idx] = item
	}
	return out
}

func encodeDiscoveryStateForWorkflow(bundle *runtimecontracts.WorkflowContractBundle, candidates []pendingCandidate) []map[string]any {
	fields := workflowSystemNodeStateSchemaFields(bundle, "discovery-aggregator")
	if len(fields) == 0 || len(candidates) == 0 {
		return []map[string]any{}
	}
	withCompat := encodePendingCandidatesForWorkflow(bundle, candidates)
	out := make([]map[string]any, 0, len(withCompat))
	for _, item := range withCompat {
		filtered := map[string]any{}
		for key := range fields {
			if value, ok := item[key]; ok {
				filtered[key] = value
			}
		}
		out = append(out, filtered)
	}
	return out
}

func cloneScoringAccumulator(acc *scoringAccumulator) *scoringAccumulator {
	if acc == nil {
		return nil
	}
	out := *acc
	out.DiscoveryContext = cloneStringAnyMap(acc.DiscoveryContext)
	out.Expected = append([]string(nil), acc.Expected...)
	out.Received = make(map[string]scoreDimensionResult, len(acc.Received))
	for key, value := range acc.Received {
		out.Received[key] = value
	}
	out.Contested = make(map[string]contestedDimension, len(acc.Contested))
	for key, value := range acc.Contested {
		copied := value
		copied.Scores = append([]int(nil), value.Scores...)
		copied.Evidence = append([]string(nil), value.Evidence...)
		copied.Options = append([]scoreDimensionResult(nil), value.Options...)
		out.Contested[key] = copied
	}
	out.ContestNotified = cloneBoolMap(acc.ContestNotified)
	return &out
}

func cloneScanAccumulator(acc *scanAccumulator) *scanAccumulator {
	if acc == nil {
		return nil
	}
	out := *acc
	out.CompletedBy = map[string]struct{}{}
	for key := range acc.CompletedBy {
		out.CompletedBy[key] = struct{}{}
	}
	out.ReportData = make([]map[string]any, 0, len(acc.ReportData))
	for _, item := range acc.ReportData {
		out.ReportData = append(out.ReportData, cloneMap(item))
	}
	return &out
}

func sliceOfMapsFromAny(raw any) []map[string]any {
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		m, ok := asObject(item)
		if ok {
			out = append(out, cloneMap(m))
		}
	}
	return out
}

func assignJSONRaw(target *json.RawMessage, raw any) {
	if target == nil || raw == nil {
		return
	}
	switch typed := raw.(type) {
	case json.RawMessage:
		*target = cloneRaw(typed)
	case []byte:
		*target = cloneRaw(typed)
	case map[string]any, []any:
		if encoded, err := json.Marshal(typed); err == nil {
			*target = encoded
		}
	}
}

func parseWorkflowTime(raw any) time.Time {
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

func stringSliceFromAny(raw any) []string {
	switch typed := raw.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if v := strings.TrimSpace(asString(item)); v != "" {
				out = append(out, v)
			}
		}
		return out
	default:
		return nil
	}
}

func boolMapFromAny(raw any) map[string]bool {
	out := map[string]bool{}
	switch typed := raw.(type) {
	case map[string]bool:
		for key, value := range typed {
			out[strings.TrimSpace(key)] = value
		}
	case map[string]any:
		for key, value := range typed {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			out[key] = truthyMetadataFlag(value)
		}
	}
	return out
}

func cloneBoolMap(in map[string]bool) map[string]bool {
	if len(in) == 0 {
		return map[string]bool{}
	}
	out := make(map[string]bool, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneMapFromAny(raw any) map[string]any {
	out, _ := asObject(raw)
	return cloneStringAnyMap(out)
}

func encodeScoreDimensionResults(in map[string]scoreDimensionResult) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = map[string]any{
			"score":      value.Score,
			"evidence":   value.Evidence,
			"confidence": value.Confidence,
		}
	}
	return out
}

func decodeScoreDimensionResults(raw any) map[string]scoreDimensionResult {
	out := map[string]scoreDimensionResult{}
	items, ok := asObject(raw)
	if !ok {
		return out
	}
	for key, value := range items {
		resultMap, ok := asObject(value)
		if !ok {
			continue
		}
		out[key] = scoreDimensionResult{
			Score:      asInt(resultMap["score"]),
			Evidence:   strings.TrimSpace(asString(resultMap["evidence"])),
			Confidence: strings.TrimSpace(asString(resultMap["confidence"])),
		}
	}
	return out
}

func encodeContestedDimensions(in map[string]contestedDimension) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		scores := make([]any, 0, len(value.Scores))
		for _, score := range value.Scores {
			scores = append(scores, score)
		}
		evidence := make([]any, 0, len(value.Evidence))
		for _, item := range value.Evidence {
			evidence = append(evidence, item)
		}
		options := make([]any, 0, len(value.Options))
		for _, option := range value.Options {
			options = append(options, map[string]any{
				"score":      option.Score,
				"evidence":   option.Evidence,
				"confidence": option.Confidence,
			})
		}
		out[key] = map[string]any{
			"dimension": value.Dimension,
			"scores":    scores,
			"evidence":  evidence,
			"spread":    value.Spread,
			"options":   options,
		}
	}
	return out
}

func decodeContestedDimensions(raw any) map[string]contestedDimension {
	out := map[string]contestedDimension{}
	items, ok := asObject(raw)
	if !ok {
		return out
	}
	for key, value := range items {
		itemMap, ok := asObject(value)
		if !ok {
			continue
		}
		contest := contestedDimension{
			Dimension: strings.TrimSpace(firstNonEmptyString(asString(itemMap["dimension"]), key)),
			Spread:    asInt(itemMap["spread"]),
		}
		if rawScores, ok := itemMap["scores"].([]any); ok {
			for _, rawScore := range rawScores {
				contest.Scores = append(contest.Scores, asInt(rawScore))
			}
		}
		if rawEvidence, ok := itemMap["evidence"].([]any); ok {
			for _, rawItem := range rawEvidence {
				contest.Evidence = append(contest.Evidence, strings.TrimSpace(asString(rawItem)))
			}
		}
		if rawOptions, ok := itemMap["options"].([]any); ok {
			for _, rawOption := range rawOptions {
				optionMap, ok := asObject(rawOption)
				if !ok {
					continue
				}
				contest.Options = append(contest.Options, scoreDimensionResult{
					Score:      asInt(optionMap["score"]),
					Evidence:   strings.TrimSpace(asString(optionMap["evidence"])),
					Confidence: strings.TrimSpace(asString(optionMap["confidence"])),
				})
			}
		}
		out[key] = contest
	}
	return out
}
