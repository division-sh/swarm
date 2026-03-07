package tools

import (
	"encoding/json"
	"strings"

	"empireai/internal/events"
	"empireai/internal/models"
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
	if role == "empire-coordinator" && eventType == "scan.requested" {
		return preNormalizeCoordinatorScanRequestedPayload(inbound, payload)
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
	if role == "empire-coordinator" && eventType == "scan.requested" {
		return normalizeCoordinatorScanRequestedPayload(inbound, payload)
	}
	if role == "empire-coordinator" && strings.HasPrefix(eventType, "budget.") && strings.TrimSpace(string(inbound.Type)) == "budget.threshold_crossed" {
		payload["event_type"] = eventType
		if _, ok := payload["threshold_event_id"]; !ok {
			payload["threshold_event_id"] = strings.TrimSpace(inbound.ID)
		}
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

func normalizeCoordinatorScanRequestedPayload(inbound events.Event, current map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range current {
		out[k] = v
	}

	directiveText := ""
	if len(inbound.Payload) > 0 {
		var payload map[string]any
		if err := json.Unmarshal(inbound.Payload, &payload); err == nil {
			directiveText = strings.TrimSpace(asString(payload["directive_text"]))
		}
	}

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
		if categories := extractCategoryList(out); len(categories) > 0 {
			out["taxonomy_categories"] = categories
		} else {
			out["taxonomy_categories"] = []string{}
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
	delete(out, "vertical")
	delete(out, "focus")
	delete(out, "criteria")
	delete(out, "payload")
	return out
}

func preNormalizeCoordinatorScanRequestedPayload(inbound events.Event, current map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range current {
		out[k] = v
	}
	directiveText := ""
	if len(inbound.Payload) > 0 {
		var payload map[string]any
		if err := json.Unmarshal(inbound.Payload, &payload); err == nil {
			directiveText = strings.TrimSpace(asString(payload["directive_text"]))
		}
	}
	originalMode := strings.TrimSpace(asString(out["mode"]))
	originalPriority := strings.TrimSpace(asString(out["priority"]))

	if nested, ok := asObject(out["payload"]); ok {
		runtimeWarn(
			"emit-normalization",
			"flattening coordinator scan.requested nested payload event_id=%s source=%s keys=%d",
			strings.TrimSpace(inbound.ID),
			strings.TrimSpace(inbound.SourceAgent),
			len(nested),
		)
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
		inferred := inferDiscoveryMode(directiveText)
		if inferred != "" {
			runtimeWarn(
				"emit-normalization",
				"coercing invalid coordinator mode raw=%q inferred=%q event_id=%s",
				strings.TrimSpace(modeRaw),
				inferred,
				strings.TrimSpace(inbound.ID),
			)
		}
		out["mode"] = inferred
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
	if coercedMode := strings.TrimSpace(asString(out["mode"])); originalMode != "" && coercedMode != "" && !strings.EqualFold(originalMode, coercedMode) {
		runtimeWarn(
			"emit-normalization",
			"coordinator scan.requested mode normalized raw=%q normalized=%q event_id=%s",
			originalMode,
			coercedMode,
			strings.TrimSpace(inbound.ID),
		)
	}
	if coercedPriority := strings.TrimSpace(asString(out["priority"])); originalPriority != "" && coercedPriority != "" && !strings.EqualFold(originalPriority, coercedPriority) {
		runtimeWarn(
			"emit-normalization",
			"coordinator scan.requested priority normalized raw=%q normalized=%q event_id=%s",
			originalPriority,
			coercedPriority,
			strings.TrimSpace(inbound.ID),
		)
	}

	delete(out, "vertical")
	delete(out, "focus")
	delete(out, "criteria")
	delete(out, "payload")
	return out
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

func normalizeScanModeCompat(raw string) string {
	if mode := normalizeScanMode(raw); mode != "" {
		return mode
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "discovery", "scan", "default", "automation", "micro", "automation-micro", "automation_micro":
		return "saas_gap"
	case "saas":
		return "saas_gap"
	case "trend", "trend_scan", "saas-trend":
		return "saas_trend"
	case "local", "local_service", "local-services", "services":
		return "local_services"
	case "corpus_mode", "signal_corpus", "corpus":
		return "corpus"
	case "derived":
		return "derived"
	default:
		return ""
	}
}

func normalizeScanPriorityCompat(raw string) string {
	if priority := normalizeScanPriority(raw); priority != "" {
		return priority
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "med", "medium", "default":
		return "normal"
	case "urgent":
		return "critical"
	default:
		return ""
	}
}

func asObject(v any) (map[string]any, bool) {
	switch t := v.(type) {
	case map[string]any:
		return t, true
	default:
		return nil, false
	}
}
