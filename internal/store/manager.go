package store

import (
	"encoding/json"
	"regexp"
	"strings"

	"empireai/internal/events"
	runtimeactors "empireai/internal/runtime/actors"
)

func mergeAgentConfigJSON(cfg runtimeactors.AgentConfig) ([]byte, error) {
	obj := map[string]any{}
	if len(cfg.Config) > 0 && json.Valid(cfg.Config) {
		_ = json.Unmarshal(cfg.Config, &obj)
	}
	if len(cfg.Subscriptions) > 0 {
		obj["subscriptions"] = cfg.Subscriptions
	}
	if _, ok := obj["role"]; !ok && cfg.Role != "" {
		obj["role"] = cfg.Role
	}
	if _, ok := obj["mode"]; !ok && cfg.Mode != "" {
		obj["mode"] = cfg.Mode
	}
	if len(obj) == 0 {
		obj = map[string]any{}
	}
	return json.Marshal(obj)
}

func extractSubscriptions(raw []byte) []string {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	var obj struct {
		Subscriptions []string `json:"subscriptions"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	return obj.Subscriptions
}

func normalizeJSONPayload(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	if json.Valid(raw) {
		var v any
		if err := json.Unmarshal(raw, &v); err == nil {
			v = redactPayloadValue("", v)
			b, err := json.Marshal(v)
			if err == nil {
				return string(b)
			}
		}
		return string(raw)
	}
	b, _ := json.Marshal(map[string]string{"raw": redactText(string(raw))})
	return string(b)
}

func matchesAnySubscription(eventType string, patterns []events.EventType) bool {
	for _, p := range patterns {
		if subscriptionMatch(string(p), eventType) {
			return true
		}
	}
	return false
}

func subscriptionMatch(pattern, eventType string) bool {
	switch {
	case pattern == "", pattern == "*":
		return true
	case strings.HasSuffix(pattern, "*"):
		return strings.HasPrefix(eventType, strings.TrimSuffix(pattern, "*"))
	default:
		return pattern == eventType
	}
}

func nullable(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func sanitizeSchemaIdent(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func quoteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

var (
	emailRegex = regexp.MustCompile(`(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b`)
	// Match likely phone formats while avoiding ISO timestamps (e.g. 2026-02-21T02:47:05Z).
	phoneRegex      = regexp.MustCompile(`(?:\+\d[\d\s().-]{7,}\d|\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b|\(\d{3}\)\s*\d{3}[-.\s]\d{4}\b)`)
	paymentRefRegex = regexp.MustCompile(`\b(?:pi|pm|ch|cs|txn|tx|tr|pay)_[a-zA-Z0-9]{6,}\b`)
)

func redactPayloadValue(key string, v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = redactPayloadValue(strings.ToLower(strings.TrimSpace(k)), vv)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i := range t {
			out[i] = redactPayloadValue(key, t[i])
		}
		return out
	case string:
		if isNameKey(key) {
			return redactName(t)
		}
		if isPaymentKey(key) && strings.TrimSpace(t) != "" {
			return "[PAYMENT_REF]"
		}
		return redactText(t)
	default:
		return v
	}
}

func redactText(s string) string {
	s = strings.ToValidUTF8(s, "\uFFFD")
	s = emailRegex.ReplaceAllString(s, "[EMAIL]")
	s = phoneRegex.ReplaceAllString(s, "[PHONE]")
	s = paymentRefRegex.ReplaceAllString(s, "[PAYMENT_REF]")
	return strings.ToValidUTF8(s, "\uFFFD")
}

func redactName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return name
	}
	runes := []rune(name)
	if len(runes) == 0 {
		return name
	}
	return strings.ToUpper(string(runes[0])) + "."
}

func isNameKey(k string) bool {
	switch k {
	case "name", "full_name", "customer_name", "first_name", "last_name":
		return true
	default:
		return false
	}
}

func isPaymentKey(k string) bool {
	k = strings.ToLower(strings.TrimSpace(k))
	if k == "" {
		return false
	}
	for _, needle := range []string{
		"payment", "transaction", "charge", "invoice", "billing", "checkout",
		"payment_ref", "payment_reference", "payment_id", "transaction_id",
	} {
		if strings.Contains(k, needle) {
			return true
		}
	}
	return false
}
