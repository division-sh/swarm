package empire

import (
	"sort"
	"strings"

	"empireai/internal/runtime/sharedjson"
)

var (
	roleTokens = []string{"owner", "operator", "manager", "founder", "director", "admin", "coordinator", "lead"}
	cohortTokens = []string{"clinic", "dental", "restaurant", "salon", "agency", "smb", "small business", "warehouse", "logistics"}
	workflowTokens = []string{"schedule", "booking", "invoice", "payroll", "lead", "dispatch", "inventory", "compliance", "reporting"}
	blockingRedFlagTypes = map[string]struct{}{
		"phone_led_sales":            {},
		"enterprise_procurement":     {},
		"relationship_networking":    {},
		"physical_presence_required": {},
		"support_mode_phone_video":   {},
	}
)

func EvaluateDiscoveryPreFilter(payload map[string]any, rawSignal float64) (bool, float64, string) {
	signal := applyRedFlagPenalty(rawSignal, payload)
	if signal < 55 {
		return false, signal, "signal_below_threshold"
	}
	if reason := blockingRedFlagReason(payload); reason != "" {
		return false, signal, reason
	}
	if !hasStructuredDiscoveryContext(payload) {
		return true, signal, ""
	}
	if !passesICPPositiveCheck(payload) {
		return false, signal, "icp_positive_check_failed"
	}
	if !passesEvidenceCompleteness(payload) {
		return false, signal, "evidence_insufficient"
	}
	if !passesRetentionPrimitive(payload) {
		return false, signal, "no_retention_primitive"
	}
	return true, signal, ""
}

func BuildPrefilterSkipDetail(payload map[string]any, rawSignal, adjustedSignal float64, reason, mode string) map[string]any {
	evidence, _ := asObject(payload["evidence"])
	competitors, _ := asArray(evidence["competitors"])
	buyerCommunities, _ := asArray(evidence["buyer_communities"])
	painSignals, _ := asArray(evidence["pain_signals"])
	regulatory, _ := asArray(evidence["regulatory"])
	evidenceURLs := collectEvidenceURLs(competitors, buyerCommunities, painSignals, regulatory)
	urls := make([]string, 0, len(evidenceURLs))
	for url := range evidenceURLs {
		urls = append(urls, url)
	}
	sort.Strings(urls)
	retentionPrimitives := extractRetentionPrimitives(payload)
	return map[string]any{
		"skip_reason":             strings.TrimSpace(reason),
		"mode":                    strings.TrimSpace(mode),
		"raw_signal_strength":     rawSignal,
		"signal_strength":         adjustedSignal,
		"red_flags":               extractRedFlagTypes(payload),
		"evidence_urls":           urls,
		"retention_primitive":     retentionPrimitives,
		"opportunity_name":        strings.TrimSpace(sharedjson.AsString(payload["opportunity_name"])),
		"opportunity_pattern":     strings.TrimSpace(sharedjson.AsString(payload["opportunity_pattern"])),
		"passes_icp_gate":         passesICPPositiveCheck(payload),
		"passes_evidence_gate":    passesEvidenceCompleteness(payload),
		"passes_retention_gate":   len(retentionPrimitives) > 0,
		"structured_context":      hasStructuredDiscoveryContext(payload),
		"blocking_red_flags_gate": hasBlockingRedFlags(payload),
	}
}

func applyRedFlagPenalty(signal float64, payload map[string]any) float64 {
	flags := extractRedFlagTypes(payload)
	if len(flags) == 0 {
		return signal
	}
	penalized := signal - float64(len(flags)*5)
	if penalized < 0 {
		return 0
	}
	if penalized > 100 {
		return 100
	}
	return penalized
}

func hasBlockingRedFlags(payload map[string]any) bool { return blockingRedFlagReason(payload) != "" }

func blockingRedFlagReason(payload map[string]any) string {
	flags := extractRedFlagTypes(payload)
	flagSet := make(map[string]struct{}, len(flags))
	for _, flag := range flags {
		flagSet[flag] = struct{}{}
		if _, blocked := blockingRedFlagTypes[flag]; blocked {
			return "blocking_red_flag"
		}
	}
	_, hasComplexIntegration := flagSet["complex_integration"]
	_, hasHighFeatureCount := flagSet["high_feature_count"]
	_, hasMultiModule := flagSet["multi_module"]
	if hasComplexIntegration && (hasHighFeatureCount || hasMultiModule) {
		return "co_occurrence_block"
	}
	return ""
}

func extractRedFlagTypes(payload map[string]any) []string {
	if len(payload) == 0 {
		return nil
	}
	buildSketch, _ := asObject(payload["build_sketch"])
	redFlags, _ := asArray(buildSketch["red_flags"])
	out := make([]string, 0, len(redFlags))
	for _, item := range redFlags {
		switch typed := item.(type) {
		case string:
			if v := strings.TrimSpace(typed); v != "" {
				out = append(out, strings.ToLower(v))
			}
		case map[string]any:
			if v := strings.TrimSpace(sharedjson.AsString(typed["type"])); v != "" {
				out = append(out, strings.ToLower(v))
			}
		}
	}
	return out
}

func hasStructuredDiscoveryContext(payload map[string]any) bool {
	if len(payload) == 0 {
		return false
	}
	if strings.TrimSpace(sharedjson.AsString(payload["opportunity_name"])) != "" {
		return true
	}
	if strings.TrimSpace(sharedjson.AsString(payload["preliminary_icp"])) != "" {
		return true
	}
	if buildSketch, ok := asObject(payload["build_sketch"]); ok && len(buildSketch) > 0 {
		return true
	}
	if evidence, ok := asObject(payload["evidence"]); ok && len(evidence) > 0 {
		return true
	}
	return false
}

func passesICPPositiveCheck(payload map[string]any) bool {
	icp := strings.ToLower(strings.TrimSpace(sharedjson.AsString(payload["preliminary_icp"])))
	hypothesis := strings.ToLower(strings.TrimSpace(sharedjson.AsString(payload["opportunity_hypothesis"])))
	text := strings.TrimSpace(icp + " " + hypothesis)
	if text == "" {
		return false
	}
	hasRole := containsAnyToken(text, roleTokens)
	hasCohort := containsAnyToken(text, cohortTokens)
	hasWorkflow := containsAnyToken(text, workflowTokens)
	if !(hasRole || hasCohort) || !hasWorkflow {
		return false
	}
	evidence, _ := asObject(payload["evidence"])
	communities, _ := asArray(evidence["buyer_communities"])
	for _, item := range communities {
		obj, _ := asObject(item)
		if isURLLike(sharedjson.AsString(obj["source_url"])) {
			return true
		}
	}
	return false
}

func passesEvidenceCompleteness(payload map[string]any) bool {
	evidence, ok := asObject(payload["evidence"])
	if !ok || len(evidence) == 0 {
		return false
	}
	competitors, _ := asArray(evidence["competitors"])
	buyerCommunities, _ := asArray(evidence["buyer_communities"])
	painSignals, _ := asArray(evidence["pain_signals"])
	if !hasCompetitorEvidence(competitors) || !hasSourceURL(buyerCommunities) || !hasSourceURL(painSignals) {
		return false
	}
	regulatory, _ := asArray(evidence["regulatory"])
	return len(collectEvidenceURLs(competitors, buyerCommunities, painSignals, regulatory)) >= 2
}

func hasCompetitorEvidence(items []any) bool {
	for _, item := range items {
		obj, _ := asObject(item)
		if strings.TrimSpace(sharedjson.AsString(obj["name"])) == "" || strings.TrimSpace(sharedjson.AsString(obj["pricing"])) == "" {
			continue
		}
		if !isURLLike(sharedjson.AsString(obj["source_url"])) {
			continue
		}
		return true
	}
	return false
}

func hasSourceURL(items []any) bool {
	for _, item := range items {
		obj, _ := asObject(item)
		if isURLLike(sharedjson.AsString(obj["source_url"])) {
			return true
		}
	}
	return false
}

func collectEvidenceURLs(parts ...[]any) map[string]struct{} {
	out := make(map[string]struct{})
	for _, items := range parts {
		for _, item := range items {
			obj, _ := asObject(item)
			raw := strings.TrimSpace(strings.ToLower(sharedjson.AsString(obj["source_url"])))
			if isURLLike(raw) {
				out[raw] = struct{}{}
			}
		}
	}
	return out
}

func passesRetentionPrimitive(payload map[string]any) bool { return len(extractRetentionPrimitives(payload)) > 0 }

func extractRetentionPrimitives(payload map[string]any) []string {
	keys := []string{"recurring_data", "workflow_embedding", "integration_lock_in", "compliance_cadence", "team_collaboration"}
	out := make(map[string]struct{}, len(keys))
	add := func(key string) {
		token := strings.TrimSpace(strings.ToLower(key))
		if token != "" {
			out[token] = struct{}{}
		}
	}
	for _, key := range keys {
		if parseBool(payload[key]) {
			add(key)
		}
	}
	buildSketch, _ := asObject(payload["build_sketch"])
	for _, key := range keys {
		if parseBool(buildSketch[key]) {
			add(key)
		}
	}
	checkArray := func(v any) {
		items, _ := asArray(v)
		for _, item := range items {
			token := strings.TrimSpace(strings.ToLower(sharedjson.AsString(item)))
			for _, key := range keys {
				if token == key {
					add(key)
				}
			}
		}
	}
	checkArray(payload["retention_primitives"])
	checkArray(buildSketch["retention_primitives"])
	for _, primitive := range inferRetentionPrimitives(payload) {
		add(primitive)
	}
	result := make([]string, 0, len(out))
	for _, key := range keys {
		if _, ok := out[key]; ok {
			result = append(result, key)
		}
	}
	return result
}

func inferRetentionPrimitives(payload map[string]any) []string {
	textParts := []string{
		strings.ToLower(strings.TrimSpace(sharedjson.AsString(payload["opportunity_hypothesis"]))),
		strings.ToLower(strings.TrimSpace(sharedjson.AsString(payload["preliminary_icp"]))),
	}
	buildSketch, _ := asObject(payload["build_sketch"])
	if coreFeatures, ok := asArray(buildSketch["core_features"]); ok {
		for _, item := range coreFeatures {
			textParts = append(textParts, strings.ToLower(strings.TrimSpace(sharedjson.AsString(item))))
		}
	}
	if integrations, ok := asArray(buildSketch["key_integrations"]); ok {
		for _, item := range integrations {
			textParts = append(textParts, strings.ToLower(strings.TrimSpace(sharedjson.AsString(item))))
		}
	}
	requiredCaps, _ := asObject(payload["required_capabilities"])
	if current, ok := asArray(requiredCaps["current"]); ok {
		for _, item := range current {
			textParts = append(textParts, strings.ToLower(strings.TrimSpace(sharedjson.AsString(item))))
		}
	}
	textParts = append(textParts, strings.ToLower(strings.TrimSpace(sharedjson.AsString(requiredCaps["would_unlock"]))))
	joined := strings.Join(textParts, " ")
	if joined == "" {
		return nil
	}
	out := make(map[string]struct{}, 5)
	add := func(primitive string) { out[primitive] = struct{}{} }
	if containsAnyToken(joined, []string{"calendar", "history", "ledger", "records", "tracking", "dashboard", "library", "portfolio", "reconciliation", "audit trail"}) {
		add("recurring_data")
	}
	if containsAnyToken(joined, []string{"workflow", "approval", "queue", "submission", "tracker", "daily", "weekly", "coordinator"}) {
		add("workflow_embedding")
	}
	if containsAnyToken(joined, []string{"integration", "sync", "api", "oauth", "quickbooks", "xero", "sage", "procore", "clio", "mri", "yardi", "erp"}) {
		add("integration_lock_in")
	}
	if containsAnyToken(joined, []string{"compliance", "deadline", "regulatory", "renewal", "expiration", "guideline", "ocg", "lien waiver", "coi"}) {
		add("compliance_cadence")
	}
	if containsAnyToken(joined, []string{"team", "partner", "manager", "attorney", "coordinator", "approval routing"}) {
		add("team_collaboration")
	}
	keys := []string{"recurring_data", "workflow_embedding", "integration_lock_in", "compliance_cadence", "team_collaboration"}
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		if _, ok := out[key]; ok {
			result = append(result, key)
		}
	}
	return result
}

func containsAnyToken(text string, tokens []string) bool {
	for _, token := range tokens {
		tok := strings.TrimSpace(strings.ToLower(token))
		if tok != "" && strings.Contains(text, tok) {
			return true
		}
	}
	return false
}

func isURLLike(raw string) bool {
	s := strings.TrimSpace(strings.ToLower(raw))
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

func parseBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "1", "true", "yes", "y", "on":
			return true
		default:
			return false
		}
	case int:
		return t != 0
	case int64:
		return t != 0
	case float64:
		return t != 0
	default:
		return false
	}
}
