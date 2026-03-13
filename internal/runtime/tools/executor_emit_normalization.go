package tools

import (
	"encoding/json"
	"strings"

	"empireai/internal/events"
	models "empireai/internal/runtime/actors"
)

func (e *Executor) preNormalizeEmitPayload(actor models.AgentConfig, inbound events.Event, eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	if eventType == "source.scraped" {
		return preNormalizeSourceScrapedPayload(inbound, payload)
	}
	if eventType == "vertical.derived" {
		return preNormalizeVerticalDerivedPayload(payload)
	}
	role := canonicalRuntimeRole(actor.Role)
	if out, ok := preNormalizeEmitPayloadSemantics(role, inbound, eventType, payload); ok {
		return out
	}
	if shouldFlattenLegacyNestedEmitPayload(eventType) {
		return preNormalizeLegacyNestedEmitPayload(payload)
	}
	return payload
}

func preNormalizeVerticalDerivedPayload(payload map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range payload {
		out[k] = v
	}
	if rationale, ok := out["derivation_rationale"]; ok {
		if _, isObj := rationale.(map[string]any); !isObj {
			if summary := strings.TrimSpace(asString(rationale)); summary != "" {
				out["derivation_rationale"] = map[string]any{"summary": summary}
			}
		}
	}
	return out
}

func (e *Executor) normalizeEmitPayload(actor models.AgentConfig, inbound events.Event, eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	role := canonicalRuntimeRole(actor.Role)
	if out, ok := normalizeEmitPayloadSemantics(role, inbound, eventType, payload); ok {
		payload = out
	}
	if eventType == "portfolio.digest_compiled" {
		msg := strings.TrimSpace(asString(payload["message"]))
		legacy := strings.TrimSpace(asString(payload["digest_text"]))
		switch {
		case msg == "" && legacy != "":
			payload["message"] = legacy
		case msg != "" && legacy == "":
			payload["digest_text"] = msg
		}
	}
	return payload
}

func preNormalizeEmitPayloadSemantics(role string, inbound events.Event, eventType string, payload map[string]any) (map[string]any, bool) {
	if role != "empire-coordinator" || strings.TrimSpace(eventType) != "scan.requested" {
		return nil, false
	}
	return normalizeCoordinatorScanRequestedPayload(inbound, payload, true), true
}

func normalizeEmitPayloadSemantics(role string, inbound events.Event, eventType string, payload map[string]any) (map[string]any, bool) {
	switch {
	case role == "empire-coordinator" && strings.TrimSpace(eventType) == "scan.requested":
		return normalizeCoordinatorScanRequestedPayload(inbound, payload, false), true
	case role == "empire-coordinator" && strings.HasPrefix(strings.TrimSpace(eventType), "budget.") && strings.TrimSpace(string(inbound.Type)) == "budget.threshold_crossed":
		out := cloneEmitPayload(payload)
		out["event_type"] = strings.TrimSpace(eventType)
		if _, ok := out["threshold_event_id"]; !ok {
			out["threshold_event_id"] = strings.TrimSpace(inbound.ID)
		}
		return out, true
	default:
		return nil, false
	}
}

func normalizeCoordinatorScanRequestedPayload(inbound events.Event, payload map[string]any, preSchema bool) map[string]any {
	out := cloneEmitPayload(payload)
	directiveText := emitDirectiveTextFromInbound(inbound)
	if nested, ok := asObject(out["payload"]); ok {
		for k, v := range nested {
			if existing, exists := out[k]; !exists || strings.TrimSpace(asString(existing)) == "" {
				out[k] = v
			}
		}
	}
	modeRaw := asString(out["mode"])
	if mode := NormalizeScanModeCompat(modeRaw); mode != "" {
		out["mode"] = mode
	} else if preSchema {
		if strings.TrimSpace(modeRaw) != "" {
			out["mode"] = inferEmitDiscoveryMode(directiveText)
		}
	} else {
		out["mode"] = defaultString(inferEmitDiscoveryMode(directiveText), "saas_gap")
	}
	if priority := NormalizeScanPriorityCompat(asString(out["priority"])); priority != "" {
		out["priority"] = priority
	} else if !preSchema {
		out["priority"] = "normal"
	}
	if strings.TrimSpace(asString(out["geography"])) == "" && strings.TrimSpace(asString(out["geography_id"])) == "" {
		if geo := inferEmitGeographyHint(directiveText); geo != "" {
			out["geography"] = geo
		} else if !preSchema {
			out["geography"] = "unspecified"
		}
	}
	if _, ok := out["campaign_context"]; !ok {
		mode := strings.TrimSpace(asString(out["mode"]))
		if mode == "" {
			mode = "saas_gap"
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
			"modes":             []string{mode},
			"strategic_context": strategicContext,
			"directive_id":      directiveID,
		}
	}
	if !preSchema {
		if _, ok := out["taxonomy_categories"]; !ok {
			out["taxonomy_categories"] = extractEmitCategoryList(out)
		}
	}
	delete(out, "vertical")
	delete(out, "focus")
	delete(out, "criteria")
	delete(out, "payload")
	return out
}

func NormalizeScanModeCompat(raw string) string {
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

func NormalizeScanPriorityCompat(raw string) string {
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

func shouldFlattenLegacyNestedEmitPayload(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "category.assessed",
		"trend.identified",
		"source.scraped",
		"market_research.scan_complete",
		"trend_research.scan_complete",
		"scanner.google_maps.scan_complete",
		"scanner.instagram.scan_complete",
		"scanner.reviews.scan_complete",
		"scanner.directories.scan_complete",
		"scanner.yelp.scan_complete":
		return true
	default:
		return false
	}
}

func preNormalizeLegacyNestedEmitPayload(current map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range current {
		out[k] = v
	}
	if nested, ok := asObject(out["payload"]); ok {
		runtimeWarn("emit-normalization", "flattening legacy nested emit payload keys=%d", len(nested))
		for k, v := range nested {
			if existing, exists := out[k]; !exists || strings.TrimSpace(asString(existing)) == "" {
				out[k] = v
			}
		}
	}
	delete(out, "payload")
	return out
}

func preNormalizeSourceScrapedPayload(inbound events.Event, current map[string]any) map[string]any {
	out := preNormalizeLegacyNestedEmitPayload(current)
	currentGeo := strings.TrimSpace(asString(out["geography"]))
	if !isPlaceholderGeography(currentGeo) {
		return out
	}
	if inferred := extractAssignedGeography(inbound); inferred != "" {
		out["geography"] = inferred
	}
	return out
}

func extractAssignedGeography(inbound events.Event) string {
	payload := parsePayloadMap(inbound.Payload)
	if len(payload) == 0 {
		return ""
	}
	for _, key := range []string{"geography", "geography_label"} {
		if value := strings.TrimSpace(asString(payload[key])); !isPlaceholderGeography(value) {
			return value
		}
	}
	if shard, ok := asObject(payload["shard"]); ok {
		if scope, ok := asObject(shard["scope"]); ok {
			for _, key := range []string{"geography", "geography_label"} {
				if value := strings.TrimSpace(asString(scope[key])); !isPlaceholderGeography(value) {
					return value
				}
			}
			if geoID := strings.TrimSpace(asString(scope["geography_id"])); geoID != "" {
				return geoID
			}
		}
	}
	if geoID := strings.TrimSpace(asString(payload["geography_id"])); geoID != "" {
		return geoID
	}
	return ""
}

func isPlaceholderGeography(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return true
	}
	tokens := strings.FieldsFunc(value, func(r rune) bool {
		switch r {
		case ',', '/', '|', ';':
			return true
		default:
			return false
		}
	})
	if len(tokens) == 0 {
		tokens = []string{value}
	}
	placeholder := map[string]struct{}{
		"unspecified": {},
		"unknown":     {},
		"n/a":         {},
		"na":          {},
		"none":        {},
		"null":        {},
		"-":           {},
	}
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if _, ok := placeholder[token]; !ok {
			return false
		}
	}
	return true
}

func asObject(v any) (map[string]any, bool) {
	switch t := v.(type) {
	case map[string]any:
		return t, true
	default:
		return nil, false
	}
}

func cloneEmitPayload(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func emitDirectiveTextFromInbound(inbound events.Event) string {
	if len(inbound.Payload) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(inbound.Payload, &payload); err != nil {
		return ""
	}
	if text := strings.TrimSpace(asString(payload["directive_text"])); text != "" {
		return text
	}
	if text := strings.TrimSpace(asString(payload["message"])); text != "" {
		return text
	}
	directive, _ := asObject(payload["directive"])
	if len(directive) == 0 {
		return ""
	}
	if text := strings.TrimSpace(asString(directive["text"])); text != "" {
		return text
	}
	raw, err := json.Marshal(directive)
	if err != nil {
		return ""
	}
	return string(raw)
}

func emitDirectiveTypeFromInbound(inbound events.Event) string {
	if len(inbound.Payload) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(inbound.Payload, &payload); err != nil {
		return ""
	}
	directive, _ := asObject(payload["directive"])
	return strings.TrimSpace(asString(directive["type"]))
}

func emitDirectiveRequiresScanRequest(evt events.Event) bool {
	if strings.TrimSpace(string(evt.Type)) != "system.directive" {
		return false
	}
	if strings.TrimSpace(evt.SourceAgent) == "scan-campaign-manager" {
		return true
	}
	if emitDirectiveTypeFromInbound(evt) != "" {
		return false
	}
	return strings.TrimSpace(emitDirectiveTextFromInbound(evt)) != ""
}

func inferEmitDiscoveryMode(text string) string {
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

func inferEmitGeographyHint(text string) string {
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

func extractEmitCategoryList(payload map[string]any) []string {
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

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return strings.TrimSpace(fallback)
}
