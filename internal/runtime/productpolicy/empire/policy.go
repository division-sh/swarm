package empire

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"empireai/internal/events"
	"empireai/internal/models"
	"empireai/internal/runtime/productpolicy"
	runtimesharedjson "empireai/internal/runtime/sharedjson"
)

type policy struct{}

func New() productpolicy.Policy {
	return policy{}
}

func (policy) EnforcePostTurn(role string, inbound events.Event, emitted []events.Event) error {
	inboundType := strings.TrimSpace(string(inbound.Type))
	switch {
	case role == "empire-coordinator" && inboundType == "system.directive":
		for _, evt := range emitted {
			if strings.TrimSpace(string(evt.Type)) == "scan.requested" {
				return nil
			}
		}
		return errors.New("system.directive handling must emit scan.requested via emit_scan_requested")
	case role == "empire-coordinator" && inboundType == "budget.threshold_crossed":
		for _, evt := range emitted {
			if strings.HasPrefix(strings.TrimSpace(string(evt.Type)), "budget.") {
				return nil
			}
		}
		return errors.New("budget.threshold_crossed handling must emit one budget.* event via emit_budget_* tool")
	default:
		return nil
	}
}

func (policy) AdditionalTurnRequirement(role string, inbound events.Event) string {
	switch {
	case role == "empire-coordinator" && strings.TrimSpace(string(inbound.Type)) == "system.directive":
		return "\n- REQUIRED for this turn: call emit_scan_requested exactly once (with mode, geography_id when known, and priority)."
	case role == "empire-coordinator" && strings.TrimSpace(string(inbound.Type)) == "budget.threshold_crossed":
		return "\n- REQUIRED for this turn: call exactly one emit_budget_* tool to reflect the threshold decision."
	default:
		return ""
	}
}

func (policy) ContractRemediationPrompt(role string, inbound events.Event, _ error) (string, bool) {
	switch {
	case role == "empire-coordinator" && strings.TrimSpace(string(inbound.Type)) == "system.directive":
		return "Runtime contract remediation: your prior response did not satisfy the required event emission.\n" +
			"Call emit_scan_requested exactly once now with valid arguments (include mode; include priority; include geography_id when known).\n" +
			"Do not return prose. Use the tool call now.", true
	case role == "empire-coordinator" && strings.TrimSpace(string(inbound.Type)) == "budget.threshold_crossed":
		return "Runtime contract remediation: your prior response did not satisfy the required event emission.\n" +
			"Call exactly one emit_budget_* tool now to reflect the budget decision.\n" +
			"Do not return prose. Use the tool call now.", true
	default:
		return "", false
	}
}

func (policy) PreNormalizeEmitPayload(role string, inbound events.Event, eventType string, payload map[string]any) (map[string]any, bool) {
	if role != "empire-coordinator" || strings.TrimSpace(eventType) != "scan.requested" {
		return nil, false
	}
	out := cloneMap(payload)
	directiveText := directiveTextFromInbound(inbound)
	originalMode := strings.TrimSpace(asString(out["mode"]))
	originalPriority := strings.TrimSpace(asString(out["priority"]))
	if nested, ok := asObject(out["payload"]); ok {
		for k, v := range nested {
			if existing, exists := out[k]; !exists || strings.TrimSpace(asString(existing)) == "" {
				out[k] = v
			}
		}
	}
	modeRaw := asString(out["mode"])
	if mode := normalizeScanModeCompat(modeRaw); mode != "" {
		out["mode"] = mode
	} else if strings.TrimSpace(modeRaw) != "" {
		out["mode"] = inferDiscoveryMode(directiveText)
	}
	if priority := normalizeScanPriorityCompat(asString(out["priority"])); priority != "" {
		out["priority"] = priority
	}
	if strings.TrimSpace(asString(out["geography"])) == "" && strings.TrimSpace(asString(out["geography_id"])) == "" {
		if geo := inferGeographyHint(directiveText); geo != "" {
			out["geography"] = geo
		} else {
			out["geography"] = "unspecified"
		}
	}
	if _, ok := out["campaign_context"]; !ok {
		modes := []string{strings.TrimSpace(asString(out["mode"]))}
		if modes[0] == "" {
			modes[0] = "saas_gap"
		}
		strategicContext := strings.TrimSpace(asString(out["strategic_context"]))
		if strategicContext == "" {
			strategicContext = directiveText
		}
		directiveID := strings.TrimSpace(asString(out["directive_id"]))
		if directiveID == "" {
			directiveID = strings.TrimSpace(inbound.ID)
		}
		out["campaign_context"] = map[string]any{
			"modes":             modes,
			"strategic_context": strategicContext,
			"directive_id":      directiveID,
		}
	}
	_ = originalMode
	_ = originalPriority
	delete(out, "vertical")
	delete(out, "focus")
	delete(out, "criteria")
	delete(out, "payload")
	return out, true
}

func (policy) NormalizeEmitPayload(role string, inbound events.Event, eventType string, payload map[string]any) (map[string]any, bool) {
	if role == "empire-coordinator" && strings.TrimSpace(eventType) == "scan.requested" {
		out := cloneMap(payload)
		directiveText := directiveTextFromInbound(inbound)
		mode := normalizeScanModeCompat(asString(out["mode"]))
		if mode == "" {
			mode = inferDiscoveryMode(directiveText)
		}
		if mode == "" {
			mode = "saas_gap"
		}
		out["mode"] = mode
		priority := normalizeScanPriorityCompat(asString(out["priority"]))
		if priority == "" {
			priority = "normal"
		}
		out["priority"] = priority
		if strings.TrimSpace(asString(out["geography"])) == "" && strings.TrimSpace(asString(out["geography_id"])) == "" {
			if geo := inferGeographyHint(directiveText); geo != "" {
				out["geography"] = geo
			} else {
				out["geography"] = "unspecified"
			}
		}
		if _, ok := out["taxonomy_categories"]; !ok {
			out["taxonomy_categories"] = extractCategoryList(out)
		}
		if _, ok := out["campaign_context"]; !ok {
			modes := []string{strings.TrimSpace(asString(out["mode"]))}
			if modes[0] == "" {
				modes[0] = "saas_gap"
			}
			strategicContext := strings.TrimSpace(asString(out["strategic_context"]))
			if strategicContext == "" {
				strategicContext = directiveText
			}
			directiveID := strings.TrimSpace(asString(out["directive_id"]))
			if directiveID == "" {
				directiveID = strings.TrimSpace(inbound.ID)
			}
			out["campaign_context"] = map[string]any{
				"modes":             modes,
				"strategic_context": strategicContext,
				"directive_id":      directiveID,
			}
		}
		delete(out, "vertical")
		delete(out, "focus")
		delete(out, "criteria")
		delete(out, "payload")
		return out, true
	}
	if role == "empire-coordinator" && strings.HasPrefix(strings.TrimSpace(eventType), "budget.") && strings.TrimSpace(string(inbound.Type)) == "budget.threshold_crossed" {
		out := cloneMap(payload)
		out["event_type"] = strings.TrimSpace(eventType)
		if _, ok := out["threshold_event_id"]; !ok {
			out["threshold_event_id"] = strings.TrimSpace(inbound.ID)
		}
		return out, true
	}
	return nil, false
}

func (policy) ValidateEmitTransition(role string, inbound events.Event, emitted events.Event) error {
	inboundType := strings.TrimSpace(string(inbound.Type))
	emittedType := strings.TrimSpace(string(emitted.Type))
	switch {
	case role == "empire-coordinator" && emittedType == "opco.spinup_requested":
		if inboundType != "vertical.approved" {
			return fmt.Errorf("guardrail_violation transition_violation: opco.spinup_requested requires inbound vertical.approved, got %s", inboundType)
		}
	case role == "empire-coordinator" && emittedType == "template.migration_completed":
		if inboundType != "template.migration_approved" {
			return fmt.Errorf("guardrail_violation transition_violation: template.migration_completed requires inbound template.migration_approved, got %s", inboundType)
		}
	case role == "empire-coordinator" && strings.HasPrefix(emittedType, "budget.") && inboundType == "budget.threshold_crossed":
		expected := strings.TrimSpace(string(budgetEventTypeFromThresholdPayload(inbound.Payload)))
		if expected != "" && expected != emittedType {
			return fmt.Errorf("guardrail_violation transition_violation: expected %s for inbound budget.threshold_crossed, got %s", expected, emittedType)
		}
	}
	return nil
}

func (policy) NormalizeScanMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "automation_micro", "local_services", "saas_gap", "saas_trend", "corpus", "derived":
		return strings.ToLower(strings.TrimSpace(raw))
	case "local_underserved":
		return "local_services"
	case "discovery", "scan", "default", "automation", "micro", "automation-micro", "saas":
		return "saas_gap"
	case "trend", "trend_scan", "saas-trend", "trend_opportunity", "adjacent_opportunity":
		return "saas_trend"
	case "local", "local_service", "local-services", "services":
		return "local_services"
	case "corpus_mode", "signal_corpus":
		return "corpus"
	default:
		return ""
	}
}

func (policy) NormalizeScanPriority(raw string) string {
	return normalizeScanPriorityCompat(raw)
}

func (policy) DefaultScanMode() string {
	return "local_services"
}

func (policy) DiscoveryFallbackMode() string {
	return "saas_gap"
}

func (p policy) RubricNameForScanMode(mode string) string {
	if p.EmitsCategorySignals(mode) || p.EmitsTrendSignals(mode) || normalizeScanModeCompat(mode) == "automation_micro" || normalizeScanModeCompat(mode) == "corpus" {
		return "saas"
	}
	return p.DefaultScanMode()
}

func (policy) EmitsCategorySignals(mode string) bool {
	return normalizeScanModeCompat(mode) == "saas_gap"
}

func (policy) EmitsTrendSignals(mode string) bool {
	return normalizeScanModeCompat(mode) == "saas_trend"
}

func (policy) ExpectedScannerCount(mode string) int {
	switch normalizeScanModeCompat(mode) {
	case "automation_micro", "saas_gap", "saas_trend", "corpus":
		return 1
	case "local_services":
		return 5
	default:
		return 1
	}
}

func (policy) ScanDispatchKind(mode string) string {
	switch normalizeScanModeCompat(mode) {
	case "saas_gap", "automation_micro", "derived":
		return "market"
	case "saas_trend":
		return "trend"
	case "corpus":
		return "corpus"
	case "local_services":
		return "local"
	default:
		return "market"
	}
}

func (policy) ScanShardStage(mode string) string {
	switch normalizeScanModeCompat(mode) {
	case "saas_gap":
		return "market_research"
	case "saas_trend":
		return "trend_research"
	default:
		return ""
	}
}

func (policy) IsCorpusScanMode(mode string) bool {
	return normalizeScanModeCompat(mode) == "corpus"
}

func (p policy) CampaignModesForDirective(initialMode string, explicit bool) []string {
	initialMode = p.NormalizeScanMode(initialMode)
	if initialMode == "" {
		initialMode = "saas_gap"
	}
	if explicit {
		return []string{initialMode}
	}
	cycle := []string{"saas_gap", "saas_trend", "local_services"}
	if initialMode == "corpus" {
		return []string{}
	}
	idx := 0
	for i, mode := range cycle {
		if mode == initialMode {
			idx = i
			break
		}
	}
	out := []string{initialMode}
	for i := idx + 1; i < len(cycle); i++ {
		out = append(out, cycle[i])
	}
	return out
}

func (policy) ParseDirectiveMode(text string) (mode string, explicit bool) {
	t := strings.ToLower(strings.TrimSpace(text))
	if t == "" {
		return "saas_gap", false
	}
	switch {
	case strings.Contains(t, "corpus_path"), strings.Contains(t, " mode corpus"), strings.HasPrefix(t, "corpus"), strings.Contains(t, ".jsonl"), strings.Contains(t, ", corpus"), strings.Contains(t, " corpus "):
		return "corpus", true
	case strings.Contains(t, "automation_micro"), (strings.Contains(t, "automation") && strings.Contains(t, "micro")):
		return "saas_gap", true
	case strings.Contains(t, "local_services"), strings.Contains(t, "local service"):
		return "local_services", true
	case strings.Contains(t, "saas_trend"), (strings.Contains(t, "saas") && strings.Contains(t, "trend")):
		return "saas_trend", true
	case strings.Contains(t, "saas_gap"), strings.Contains(t, "gap scan"):
		return "saas_gap", true
	default:
		return "saas_gap", false
	}
}

func (policy) InterceptRuntimeHandledDirective(agent models.AgentConfig, inbound events.Event) bool {
	if strings.TrimSpace(agent.Role) != "empire-coordinator" {
		return false
	}
	if strings.TrimSpace(string(inbound.Type)) != "system.directive" {
		return false
	}
	return !directiveRequiresCoordinator(inbound)
}

func (policy) AllowHumanTaskDecision(actor models.AgentConfig) bool {
	return strings.TrimSpace(actor.Role) == "empire-coordinator"
}

func (policy) AllowGlobalRouting(actor models.AgentConfig) bool {
	return strings.TrimSpace(actor.Role) == "empire-coordinator"
}

func (policy) AllowGlobalManagement(actor models.AgentConfig) bool {
	return strings.TrimSpace(actor.Role) == "empire-coordinator"
}

func (policy) AllowMailboxSend(actor models.AgentConfig) bool {
	return strings.TrimSpace(actor.Role) == "empire-coordinator"
}

func (policy) ManagerFallbackAgentID(agent models.AgentConfig) string {
	_ = agent
	return "empire-coordinator"
}

func (policy) WorkspaceClass(actor models.AgentConfig) string {
	role := strings.ToLower(strings.TrimSpace(actor.Role))
	switch role {
	case "holding-devops":
		return "infra"
	case "factory-cto",
		"empire-coordinator",
		"operations-analyst",
		"scanner-agent",
		"analysis-agent",
		"validation-coordinator",
		"pre-brand-agent",
		"business-research-agent",
		"lightweight-spec-agent",
		"spec-reviewer",
		"market-research-agent",
		"trend-research-agent",
		"spec-auditor",
		"discovery-coordinator":
		return "factory"
	default:
		return ""
	}
}

func (policy) DiagnosticWorkspaceClass(role string) string {
	role = strings.ToLower(strings.TrimSpace(role))
	switch role {
	case "holding-devops":
		return "infra"
	case "factory-cto",
		"empire-coordinator",
		"operations-analyst",
		"scanner-agent",
		"analysis-agent",
		"validation-coordinator",
		"pre-brand-agent",
		"business-research-agent",
		"lightweight-spec-agent",
		"spec-reviewer",
		"market-research-agent",
		"trend-research-agent",
		"spec-auditor",
		"discovery-coordinator":
		return "factory"
	default:
		return ""
	}
}

func (policy) PromptSchemaGuards() []productpolicy.PromptSchemaGuard {
	return []productpolicy.PromptSchemaGuard{
		{
			PromptFile:       "market-research-agent.md",
			EmitTool:         "emit_category_assessed",
			RequiredTopLevel: []string{"opportunity_name", "preliminary_icp", "build_sketch", "evidence", "opportunity_hypothesis", "opportunity_pattern", "signal_sources", "required_capabilities"},
			ForbiddenTokens:  []string{"automation_micro", "market_intersection", "urgency"},
		},
		{
			PromptFile:       "market-research-agent.corpus.md",
			EmitTool:         "emit_category_assessed",
			RequiredTopLevel: []string{"opportunity_name", "preliminary_icp", "build_sketch", "evidence", "opportunity_hypothesis", "opportunity_pattern", "signal_sources", "required_capabilities"},
			ForbiddenTokens:  []string{"automation_micro", "market_intersection", "urgency"},
		},
		{
			PromptFile:       "trend-research-agent.md",
			EmitTool:         "emit_trend_identified",
			RequiredTopLevel: []string{"opportunity_name", "preliminary_icp", "build_sketch", "evidence", "trend_description", "opportunity_hypothesis", "geographic_scope"},
			ForbiddenTokens:  []string{"market_intersection", "urgency"},
		},
	}
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

func directiveTextFromInbound(inbound events.Event) string {
	if len(inbound.Payload) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(inbound.Payload, &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(asString(payload["directive_text"]))
}

func directiveRequiresCoordinator(evt events.Event) bool {
	if strings.TrimSpace(evt.SourceAgent) == "scan-campaign-manager" {
		return true
	}
	text := directiveTextFromInbound(evt)
	if text == "" {
		return false
	}
	return isComplexDirectiveText(text)
}

func inferDiscoveryMode(text string) string {
	t := strings.ToLower(strings.TrimSpace(text))
	switch {
	case strings.Contains(t, "automation_micro"),
		(strings.Contains(t, "automation") && strings.Contains(t, "micro")):
		return "saas_gap"
	case strings.Contains(t, "local service"), strings.Contains(t, "local_services"):
		return "local_services"
	case strings.Contains(t, "trend"), strings.Contains(t, "saas_trend"):
		return "saas_trend"
	default:
		return "saas_gap"
	}
}

func inferGeographyHint(text string) string {
	t := strings.TrimSpace(text)
	if t == "" {
		return ""
	}
	low := strings.ToLower(t)
	for _, geo := range []string{"paraguay", "argentina", "brazil", "mexico", "chile", "peru", "colombia", "uruguay"} {
		if strings.Contains(low, geo) {
			return geo
		}
	}
	return t
}

func isComplexDirectiveText(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	triggerPhrases := []string{
		"adjacent",
		"bundle",
		"bundles",
		"compare",
		"comparison",
		"compliance",
		"countries",
		"country",
		"cross-border",
		"dataset",
		"focus on",
		"internet penetration",
		"latam",
		"multi-",
		"multiple",
		"opportunities",
		"portfolio",
		"prioritize",
		"priority",
		"regions",
		"segment",
		"segmentation",
		"verticals",
	}
	for _, phrase := range triggerPhrases {
		if strings.Contains(text, phrase) {
			return true
		}
	}
	return false
}

func extractCategoryList(payload map[string]any) []string {
	toList := func(v any) []string {
		switch t := v.(type) {
		case []any:
			out := make([]string, 0, len(t))
			for _, item := range t {
				s := strings.TrimSpace(asString(item))
				if s != "" {
					out = append(out, s)
				}
			}
			return out
		case []string:
			out := make([]string, 0, len(t))
			for _, item := range t {
				s := strings.TrimSpace(item)
				if s != "" {
					out = append(out, s)
				}
			}
			return out
		default:
			return nil
		}
	}
	if out := toList(payload["taxonomy_categories"]); len(out) > 0 {
		return out
	}
	if out := toList(payload["categories"]); len(out) > 0 {
		return out
	}
	return []string{}
}

func budgetEventTypeFromThresholdPayload(raw []byte) events.EventType {
	state := strings.ToLower(strings.TrimSpace(fieldStringFromJSON(raw, "state")))
	switch state {
	case "emergency":
		return events.EventType("budget.emergency")
	case "throttle":
		return events.EventType("budget.throttle")
	case "warning":
		return events.EventType("budget.warning")
	case "ok", "resumed":
		return events.EventType("budget.resumed")
	default:
		return ""
	}
}

func fieldStringFromJSON(raw []byte, key string) string {
	if len(raw) == 0 || strings.TrimSpace(key) == "" {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return ""
	}
	return strings.TrimSpace(asString(obj[key]))
}

func normalizeScanModeCompat(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "saas_gap", "saas_trend", "local_services", "corpus", "derived":
		return strings.ToLower(strings.TrimSpace(raw))
	case "discovery", "scan", "default", "automation", "micro", "automation-micro", "automation_micro", "saas":
		return "saas_gap"
	case "trend", "trend_scan", "saas-trend":
		return "saas_trend"
	case "local", "local_service", "local-services", "services":
		return "local_services"
	case "corpus_mode", "signal_corpus":
		return "corpus"
	default:
		return ""
	}
}

func normalizeScanPriorityCompat(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "low", "normal", "high", "critical":
		return strings.ToLower(strings.TrimSpace(raw))
	case "med", "medium", "default":
		return "normal"
	case "urgent":
		return "critical"
	default:
		return ""
	}
}

func asObject(v any) (map[string]any, bool) {
	obj, ok := v.(map[string]any)
	return obj, ok
}

func asString(v any) string {
	return runtimesharedjson.AsString(v)
}
