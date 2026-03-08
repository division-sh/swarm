package tools

import (
	"strings"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
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
	if policy := runtimeproductpolicy.DefaultOrNil(); policy != nil {
		if out, ok := policy.PreNormalizeEmitPayload(role, inbound, eventType, payload); ok {
			return out
		}
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
	if policy := runtimeproductpolicy.DefaultOrNil(); policy != nil {
		if out, ok := policy.NormalizeEmitPayload(role, inbound, eventType, payload); ok {
			payload = out
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
