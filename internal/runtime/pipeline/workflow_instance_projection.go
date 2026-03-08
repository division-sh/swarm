package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

func (pc *FactoryPipelineCoordinator) persistWorkflowScoringAccumulator(ctx context.Context, acc *scoringAccumulator) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || acc == nil {
		return
	}
	verticalID := strings.TrimSpace(acc.VerticalID)
	if verticalID == "" {
		return
	}
	encoded := encodeScoringAccumulator(acc)
	_ = pc.workflowStore.Mutate(ctx, verticalID, func(instance *WorkflowInstance) {
		if instance.AccumulatorState == nil {
			instance.AccumulatorState = map[string]any{}
		}
		instance.AccumulatorState["scoring-state"] = encoded
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
		delete(instance.AccumulatorState, "scoring-state")
	})
}

func restoreValidationStateFromInstance(instance WorkflowInstance) (*validationPipelineState, bool) {
	metadata := cloneStringAnyMap(instance.Metadata)
	bucket, ok := asObject(instance.AccumulatorState["pipeline-coordinator"])
	if ok {
		for key, value := range bucket {
			if _, exists := metadata[key]; !exists {
				metadata[key] = value
			}
		}
	}
	if len(metadata) == 0 {
		return nil, false
	}
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
	bucket, ok := asObject(instance.AccumulatorState["scoring-state"])
	if !ok || len(bucket) == 0 {
		return nil, false
	}
	verticalID := strings.TrimSpace(firstNonEmptyString(asString(bucket["vertical_id"]), instance.InstanceID))
	if verticalID == "" {
		return nil, false
	}
	acc := &scoringAccumulator{
		VerticalID:       verticalID,
		VerticalName:     strings.TrimSpace(asString(bucket["vertical_name"])),
		Geography:        strings.TrimSpace(asString(bucket["geography"])),
		GeographicScope:  strings.TrimSpace(asString(bucket["geographic_scope"])),
		Mode:             normalizeScanMode(asString(bucket["mode"])),
		Rubric:           strings.TrimSpace(asString(bucket["rubric"])),
		DiscoveryContext: cloneMapFromAny(bucket["discovery_context"]),
		Expected:         stringSliceFromAny(bucket["expected"]),
		Received:         decodeScoreDimensionResults(bucket["received"]),
		Contested:        decodeContestedDimensions(bucket["contested"]),
		RequestedAt:      parseWorkflowTime(bucket["requested_at"]),
		LastUpdatedAt:    parseWorkflowTime(bucket["last_updated_at"]),
		ContestNotified:  boolMapFromAny(bucket["contest_notified"]),
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
