package empire

import (
	"encoding/json"
	"strings"
	"time"

	"empireai/internal/runtime/sharedjson"
)

func BuildScanAssignedPayload(
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
		GeographyID:        strings.TrimSpace(sharedjson.AsString(source["geography_id"])),
		TaxonomyCategories: source["taxonomy_categories"],
		Priority:           strings.TrimSpace(sharedjson.AsString(source["priority"])),
		CampaignContext:    source["campaign_context"],
		DirectiveID:        strings.TrimSpace(sharedjson.AsString(source["directive_id"])),
		StrategicContext:   source["strategic_context"],
		CorpusPath:         strings.TrimSpace(sharedjson.AsString(source["corpus_path"])),
		CorpusSignals:      source["corpus_signals"],
		RequestedAt:        time.Now().UTC().Format(time.RFC3339),
		PlannedShards:      plannedShards,
	}
}

func BuildSynthesisNeededPayload(scanID, campaignID, mode, geography string, raw map[string]any) SynthesisNeededPayload {
	if raw == nil {
		raw = map[string]any{}
	}
	return SynthesisNeededPayload{
		ScanID:        strings.TrimSpace(scanID),
		CampaignID:    strings.TrimSpace(campaignID),
		Mode:          strings.TrimSpace(mode),
		Geography:     strings.TrimSpace(geography),
		Category:      strings.TrimSpace(sharedjson.AsString(raw["category"])),
		Subcategory:   strings.TrimSpace(sharedjson.AsString(raw["subcategory"])),
		ConflictNotes: raw["conflict_notes"],
		RawReport:     raw,
	}
}

func BuildDedupAmbiguousPayload(
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

func BuildVerticalDiscoveredPayload(
	verticalID, name, geography, mode, scanID, campaignID string,
	signal float64,
	discoverySource string,
	rawSignals map[string]any,
) VerticalDiscoveredPayload {
	if rawSignals == nil {
		rawSignals = map[string]any{}
	}
	discoveryContext := buildDiscoveryContextPayload(rawSignals)
	geographicScope := normalizeGeographicScope(sharedjson.AsString(rawSignals["geographic_scope"]))
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
		OpportunityPattern:   normalizeOpportunityPattern(sharedjson.AsString(rawSignals["opportunity_pattern"])),
		SignalSources:        rawSignals["signal_sources"],
		RequiredCapabilities: rawSignals["required_capabilities"],
		DiscoverySource:      strings.TrimSpace(discoverySource),
		RawSignals:           rawSignals,
		DiscoveryContext:     discoveryContext,
	}
}

func BuildDiscoveryContextPayload(raw map[string]any) map[string]any {
	return buildDiscoveryContextPayload(raw)
}

func NormalizeOpportunityPattern(raw string) string {
	return normalizeOpportunityPattern(raw)
}

func NormalizeGeographicScope(raw string) string {
	return normalizeGeographicScope(raw)
}

func BuildScanCompletedPayload(in ScanCompletedBuildInput) ScanCompletedPayload {
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
		ShardsTotal:     in.ShardsTotal,
		ShardsCompleted: in.ShardsCompleted,
		ShardsFailed:    in.ShardsFailed,
	}
}

func buildDiscoveryContextPayload(raw map[string]any) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	out := map[string]any{}
	if v := strings.TrimSpace(sharedjson.AsString(raw["opportunity_name"])); v != "" {
		out["opportunity_name"] = v
	}
	if v := strings.TrimSpace(sharedjson.AsString(raw["preliminary_icp"])); v != "" {
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
	if v := strings.TrimSpace(sharedjson.AsString(raw["opportunity_hypothesis"])); v != "" {
		out["opportunity_hypothesis"] = v
	}
	if v := normalizeOpportunityPattern(sharedjson.AsString(raw["opportunity_pattern"])); v != "" {
		out["opportunity_pattern"] = v
	}
	if sources := raw["signal_sources"]; sources != nil {
		out["signal_sources"] = sources
	}
	if caps := raw["required_capabilities"]; caps != nil {
		out["required_capabilities"] = caps
	}
	if parentID := strings.TrimSpace(sharedjson.AsString(raw["parent_id"])); parentID != "" {
		out["parent_id"] = parentID
	}
	if depth := asInt(raw["generation_depth"]); depth > 0 {
		out["generation_depth"] = depth
	}
	if generator := strings.TrimSpace(sharedjson.AsString(raw["generator_agent_id"])); generator != "" {
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
		if text := strings.TrimSpace(sharedjson.AsString(t["questions"])); text != "" {
			return text
		}
		if text := strings.TrimSpace(sharedjson.AsString(t["cto_feedback"])); text != "" {
			return text
		}
		if text := strings.TrimSpace(sharedjson.AsString(t["feedback"])); text != "" {
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
	return strings.TrimSpace(sharedjson.AsString(v))
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

func asObject(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return nil, false
	}
	return m, true
}

func asArray(v any) ([]any, bool) {
	items, ok := v.([]any)
	if !ok || len(items) == 0 {
		return nil, false
	}
	return items, true
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func parsePayloadMap(raw any) map[string]any {
	switch t := raw.(type) {
	case nil:
		return map[string]any{}
	case []byte:
		return sharedjson.ParsePayloadMap(t)
	case map[string]any:
		return cloneMap(t)
	default:
		if b, err := json.Marshal(t); err == nil {
			return sharedjson.ParsePayloadMap(b)
		}
		return map[string]any{}
	}
}

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case json.Number:
		n, _ := t.Int64()
		return int(n)
	case string:
		n, _ := json.Number(strings.TrimSpace(t)).Int64()
		return int(n)
	default:
		return 0
	}
}
